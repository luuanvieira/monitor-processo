// Package postgres implementa armazenamento.Store sobre PostgreSQL com pgx.
package postgres

import (
	"context"
	_ "embed"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/luuanvieira/monitor-processo/internal/alarmes"
	"github.com/luuanvieira/monitor-processo/internal/processo"
)

//go:embed schema.sql
var schema string

// Store é seguro para uso concorrente (o pool gerencia as conexões).
type Store struct {
	pool *pgxpool.Pool
}

// Conectar abre o pool, testa a conexão e aplica o esquema.
func Conectar(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("postgres: configuração inválida: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: sem conexão: %w", err)
	}
	if err := s.migrar(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrar(ctx context.Context) error {
	for _, instrucao := range strings.Split(schema, ";") {
		if strings.TrimSpace(semComentarios(instrucao)) == "" {
			continue
		}
		if _, err := s.pool.Exec(ctx, instrucao); err != nil {
			return fmt.Errorf("postgres: aplicando esquema: %w", err)
		}
	}
	return nil
}

func semComentarios(sql string) string {
	var b strings.Builder
	for _, linha := range strings.Split(sql, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(linha), "--") {
			b.WriteString(linha)
			b.WriteByte('\n')
		}
	}
	return b.String()
}

// SalvarLeituras grava o lote inteiro em uma única instrução (unnest de arrays).
// Leituras repetidas (mesmo tag e momento) são ignoradas, o que torna o
// reenvio de um lote seguro.
func (s *Store) SalvarLeituras(ctx context.Context, ls []processo.Leitura) error {
	if len(ls) == 0 {
		return nil
	}
	tags := make([]string, len(ls))
	momentos := make([]time.Time, len(ls))
	valores := make([]float64, len(ls))
	qualidades := make([]int16, len(ls))
	for i, l := range ls {
		tags[i], momentos[i], valores[i], qualidades[i] = l.Tag, l.Momento, l.Valor, int16(l.Qualidade)
	}
	_, err := s.pool.Exec(ctx, `
		insert into leituras (tag, momento, valor, qualidade)
		select * from unnest($1::text[], $2::timestamptz[], $3::float8[], $4::int2[])
		on conflict (tag, momento) do nothing`,
		tags, momentos, valores, qualidades)
	return err
}

func (s *Store) SalvarEventos(ctx context.Context, ev []alarmes.Evento) error {
	if len(ev) == 0 {
		return nil
	}
	n := len(ev)
	seqs, momentos, valores := make([]int64, n), make([]time.Time, n), make([]float64, n)
	tags, tipos, prioridades, estados, mensagens := make([]string, n), make([]string, n), make([]string, n), make([]string, n), make([]string, n)
	for i, e := range ev {
		seqs[i], momentos[i], valores[i] = int64(e.Seq), e.Momento, e.Valor
		tags[i], tipos[i], prioridades[i] = e.Tag, string(e.Tipo), string(e.Prioridade)
		estados[i], mensagens[i] = string(e.Estado), e.Mensagem
	}
	_, err := s.pool.Exec(ctx, `
		insert into eventos_alarme (seq, momento, tag, tipo, prioridade, estado, valor, mensagem)
		select * from unnest($1::int8[], $2::timestamptz[], $3::text[], $4::text[],
		                     $5::text[], $6::text[], $7::float8[], $8::text[])`,
		seqs, momentos, tags, tipos, prioridades, estados, valores, mensagens)
	return err
}

func (s *Store) Leituras(ctx context.Context, tag string, desde, ate time.Time, limite int) ([]processo.Leitura, error) {
	if limite <= 0 {
		limite = math.MaxInt32
	}
	rows, err := s.pool.Query(ctx, `
		select tag, momento, valor, qualidade from (
			select tag, momento, valor, qualidade
			from leituras
			where tag = $1 and momento between $2 and $3
			order by momento desc
			limit $4
		) recentes
		order by momento`,
		tag, desde, ate, limite)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []processo.Leitura{}
	for rows.Next() {
		var l processo.Leitura
		var q int16
		if err := rows.Scan(&l.Tag, &l.Momento, &l.Valor, &q); err != nil {
			return nil, err
		}
		l.Qualidade = processo.Qualidade(q)
		l.Momento = l.Momento.UTC()
		out = append(out, l)
	}
	return out, rows.Err()
}

func (s *Store) Eventos(ctx context.Context, tag string, limite int) ([]alarmes.Evento, error) {
	if limite <= 0 {
		limite = math.MaxInt32
	}
	rows, err := s.pool.Query(ctx, `
		select seq, momento, tag, tipo, prioridade, estado, valor, mensagem
		from eventos_alarme
		where $1 = '' or tag = $1
		order by momento desc, id desc
		limit $2`,
		tag, limite)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []alarmes.Evento{}
	for rows.Next() {
		var e alarmes.Evento
		var seq int64
		var tipo, prioridade, estado string
		if err := rows.Scan(&seq, &e.Momento, &e.Tag, &tipo, &prioridade, &estado, &e.Valor, &e.Mensagem); err != nil {
			return nil, err
		}
		e.Seq, e.Momento = uint64(seq), e.Momento.UTC()
		e.Tipo, e.Prioridade, e.Estado = alarmes.Tipo(tipo), alarmes.Prioridade(prioridade), alarmes.Estado(estado)
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) Close() error {
	s.pool.Close()
	return nil
}
