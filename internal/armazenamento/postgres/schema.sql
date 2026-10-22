-- Esquema do histórico. Aplicado na inicialização (todas as instruções são idempotentes).

create table if not exists leituras (
    tag       text             not null,
    momento   timestamptz      not null,
    valor     double precision not null,
    qualidade smallint         not null default 0,
    primary key (tag, momento)
);

-- Índice BRIN: muito pequeno e eficiente para dados que chegam em ordem de tempo.
create index if not exists leituras_momento_brin on leituras using brin (momento);

create table if not exists eventos_alarme (
    id         bigserial        primary key,
    seq        bigint           not null,
    momento    timestamptz      not null,
    tag        text             not null,
    tipo       text             not null,
    prioridade text             not null,
    estado     text             not null,
    valor      double precision not null,
    mensagem   text             not null
);

create index if not exists eventos_alarme_momento_idx on eventos_alarme (momento desc, id desc);
create index if not exists eventos_alarme_tag_idx on eventos_alarme (tag, momento desc);
