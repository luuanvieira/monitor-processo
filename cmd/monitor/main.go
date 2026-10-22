// Comando monitor: sobe o simulador da planta, o motor de alarmes, a API e o dashboard.
//
// Variáveis de ambiente:
//
//	HTTP_ENDERECO    endereço do servidor (padrão :8080)
//	DATABASE_URL     PostgreSQL; se vazio, o histórico fica em memória
//	TAGS_ARQUIVO     JSON com a configuração dos tags (padrão: config/tags.json embutido)
//	SIMULADOR        true/false (padrão true); desligue para receber só pela API
//	SIM_VELOCIDADE   segundos simulados por segundo real (padrão 1)
//	SIM_SEMENTE      semente do simulador (padrão: aleatória)
//	LOG_NIVEL        debug, info, warn, error (padrão info)
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/luuanvieira/monitor-processo/config"
	"github.com/luuanvieira/monitor-processo/internal/api"
	"github.com/luuanvieira/monitor-processo/internal/app"
	"github.com/luuanvieira/monitor-processo/internal/armazenamento"
	"github.com/luuanvieira/monitor-processo/internal/armazenamento/postgres"
	"github.com/luuanvieira/monitor-processo/internal/processo"
)

func main() {
	if err := executar(); err != nil {
		slog.Error("encerrado com erro", "erro", err)
		os.Exit(1)
	}
}

func env(nome, padrao string) string {
	if v, ok := os.LookupEnv(nome); ok && v != "" {
		return v
	}
	return padrao
}

func executar() error {
	var nivel slog.Level
	if err := nivel.UnmarshalText([]byte(env("LOG_NIVEL", "info"))); err != nil {
		return fmt.Errorf("LOG_NIVEL: %w", err)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: nivel}))
	slog.SetDefault(log)

	tags, err := carregarTags(env("TAGS_ARQUIVO", ""))
	if err != nil {
		return err
	}
	simulador, err := strconv.ParseBool(env("SIMULADOR", "true"))
	if err != nil {
		return fmt.Errorf("SIMULADOR: %w", err)
	}
	velocidade, err := strconv.Atoi(env("SIM_VELOCIDADE", "1"))
	if err != nil || velocidade < 1 || velocidade > 600 {
		return errors.New("SIM_VELOCIDADE deve ser um inteiro entre 1 e 600")
	}
	semente, err := strconv.ParseUint(env("SIM_SEMENTE", strconv.FormatUint(rand.Uint64(), 10)), 10, 64)
	if err != nil {
		return fmt.Errorf("SIM_SEMENTE: %w", err)
	}

	ctx, parar := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer parar()

	store, err := abrirStore(ctx, env("DATABASE_URL", ""), log)
	if err != nil {
		return err
	}
	defer store.Close()

	a := app.Nova(app.Config{
		Tags: tags, Store: store, Simulador: simulador, Semente: semente,
		Velocidade: velocidade, Passo: time.Second, Log: log,
	})

	ctxApp, pararApp := context.WithCancel(context.Background())
	appEncerrado := make(chan struct{})
	go func() { a.Executar(ctxApp); close(appEncerrado) }()

	encerrando := make(chan struct{})
	srv := &http.Server{
		Addr:              env("HTTP_ENDERECO", ":8080"),
		Handler:           api.Novo(a, log, encerrando),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	srv.RegisterOnShutdown(func() { close(encerrando) })

	erroServidor := make(chan error, 1)
	go func() {
		log.Info("servidor no ar", "endereco", srv.Addr, "simulador", simulador,
			"velocidade", velocidade, "semente", semente, "tags", len(tags))
		erroServidor <- srv.ListenAndServe()
	}()

	select {
	case err := <-erroServidor:
		pararApp()
		<-appEncerrado
		return err
	case <-ctx.Done():
	}

	// Encerramento em ordem: para de aceitar requisições, depois esvazia o
	// pipeline (que grava o que estava pendente) e por fim fecha o banco (defer).
	log.Info("encerrando")
	ctxShutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctxShutdown); err != nil {
		log.Warn("shutdown do HTTP", "erro", err)
	}
	pararApp()
	<-appEncerrado
	return nil
}

func carregarTags(caminho string) ([]processo.Tag, error) {
	conteudo := config.TagsPadrao
	if caminho != "" {
		var err error
		if conteudo, err = os.ReadFile(caminho); err != nil {
			return nil, fmt.Errorf("TAGS_ARQUIVO: %w", err)
		}
	}
	return processo.CarregarTags(bytes.NewReader(conteudo))
}

func abrirStore(ctx context.Context, url string, log *slog.Logger) (armazenamento.Store, error) {
	if url == "" {
		log.Warn("DATABASE_URL vazio: histórico em memória (perdido ao reiniciar)")
		// ~2 h por tag a 1 leitura/s
		return armazenamento.NovaMemoria(7200, 10_000), nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var ultimoErro error
	// O banco pode subir depois da aplicação (docker compose): tenta por até 30 s.
	for {
		s, err := postgres.Conectar(ctx, url)
		if err == nil {
			log.Info("conectado ao PostgreSQL")
			return s, nil
		}
		ultimoErro = err
		select {
		case <-ctx.Done():
			return nil, ultimoErro
		case <-time.After(2 * time.Second):
			log.Warn("aguardando o PostgreSQL", "erro", err)
		}
	}
}
