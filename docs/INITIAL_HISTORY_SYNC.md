# Sincronização inicial do histórico

Ao escanear um QR para vincular um aparelho novo, o WhatsApp envia histórico em
lotes. Reiniciar o worker com a sessão salva reconecta o mesmo aparelho; não
repete o pedido feito no registro. A busca adicional (backfill) pede mensagens
mais antigas depois, usando conversas e mensagens já armazenadas como referência.

## Configuração para uma conta nova

O instalador prepara estas variáveis antes do QR:

| Variável | Valor gerado | Função |
| --- | --- | --- |
| `WHATSAPP_HISTORY_INITIAL_SYNC_FULL` | `true` | Pedir sincronização ampla ao registrar um aparelho novo. |
| `WHATSAPP_HISTORY_BACKFILL_DAYS` | Escolha explícita do usuário | Pedir esse período no registro e limitar até onde a busca adicional avança. |
| `WHATSAPP_HISTORY_BACKFILL_START_DELAY_SECONDS` | `90` | Esperar uma pausa de 90 segundos nos lotes iniciais antes da busca adicional. |
| `WHATSAPP_HISTORY_INITIAL_SYNC_WAIT_MAX_SECONDS` | `600` | Limitar a espera por novos lotes a dez minutos. |

Com o pedido ampliado habilitado, o bridge envia `RequireFullSync=true` antes de
conectar um aparelho ainda não registrado. Dias maiores que zero também definem
`FullSyncDaysLimit=N`. Zero deixa o limite de dias do protocolo ausente e permite
backfill sem corte.
O pedido inicial é uma preferência: o telefone pode enviar menos dados, mais
antigos ou entregar lotes depois. Não é uma prova de cobertura nem um filtro que
apaga mensagens fora do período.

O perfil usa as capacidades, plataforma e quotas padrão do Whatsmeow. Planeje
o volume e monitore armazenamento e ingestão.

Quando `WHATSAPP_HISTORY_INITIAL_SYNC_FULL` está ausente ou é `false`, o aparelho
usa o pedido inicial padrão do Whatsmeow. Com
`WHATSAPP_HISTORY_INITIAL_SYNC_WAIT_MAX_SECONDS` ausente ou igual a `0`, a busca
adicional usa o atraso fixo de `WHATSAPP_HISTORY_BACKFILL_START_DELAY_SECONDS`
(ausente ou `0`: sem atraso). Um valor positivo habilita a espera adaptativa.
Reexecutar o instalador com os mesmos dados reutiliza o export e as credenciais;
ele não altera as variáveis já preparadas.

Para uma conta já pareada, apenas adicionar a flag ou reiniciar não amplia o
pedido inicial. Não apague sessões nem force outro QR automaticamente; continue
a recuperação por checkpoints. Um novo pareamento deve ser uma ação explícita
do dono, com backups dos dados já armazenados.

## Quando começa a busca adicional

Com a espera adaptativa habilitada, o worker acompanha as notificações e lotes
iniciais dos tipos bootstrap, recent e full. Cada chegada ou gravação de lote
reinicia o período de silêncio. Mensagens novas e respostas on-demand do próprio
backfill não reiniciam essa espera inicial.

A busca adicional pode começar depois da pausa configurada e da drenagem do
trabalho recebido. O tempo máximo evita esperar indefinidamente por mais lotes,
mas não ignora downloads ou armazenamento pendentes ou com falhas. Portanto,
600 segundos não são um limite total: uma pendência pode prolongar a espera.
Falhas de download usam novas tentativas com intervalos persistidos de cinco
segundos a um minuto. Um arquivo remoto expirado pode exigir investigação;
não é tratado como prova de ausência de mensagens. O worker
continua recebendo e gravando lotes tardios mesmo depois de iniciar o backfill.

As notificações de histórico são registradas antes do download. O processamento
sequencial pode retomar downloads pendentes após reinícios e usa a fila de
ingestão para gravar o arquivo consultável. Isso protege trabalho já admitido;
não garante que o WhatsApp envie todas as conversas ou mantenha arquivos remotos
disponíveis indefinidamente.

## Como avaliar o resultado

`get_sync_status` distingue recepção inicial, trabalho pendente e busca adicional.
Considere conjuntamente atividade recente, lotes recebidos/gravados, falhas de
download e ingestão, contagens de mensagens e resultados por conversa.

O campo de progresso enviado pelo protocolo pertence àquela fase/lote. Nem
`progress=100`, fila vazia, silêncio ou cursor antigo demonstram que todas as
mensagens do período estão no arquivo. Compare mensagens conhecidas no celular
e registre as lacunas. A meta congelada do backfill fica em
`archive.backfill.requested_window`, com completude desconhecida.

## Referências de implementação

- [Very Good Plugins: pedido ampliado antes de criar o client](https://github.com/verygoodplugins/whatsapp-mcp/blob/895404542017f34a900f9f572a5497c275a96440/whatsapp-bridge/main.go#L2906-L2928)
  e [logs de tipo, chunk e progresso](https://github.com/verygoodplugins/whatsapp-mcp/blob/895404542017f34a900f9f572a5497c275a96440/whatsapp-bridge/main.go#L3485-L3495).
  Referência para os campos do pedido e os sinais de progresso do protocolo.
- [Hansen: dias configuráveis no pareamento](https://github.com/hansen1101/whatsapp-mcp-server/blob/275f6b809dfbc888fd20be9d7924ac9cb0691148/main.go#L168-L173)
  e [espera desde o último lote](https://github.com/hansen1101/whatsapp-mcp-server/blob/275f6b809dfbc888fd20be9d7924ac9cb0691148/main.go#L232-L261).
  Referência para aguardar uma pausa na chegada de lotes.
- [WAHA GOWS: controles experimentais de histórico](https://waha.devlike.pro/docs/engines/gows/).
  O período pedido não equivale a uma garantia de entrega pelo WhatsApp.

- [Mautrix: persistência do aviso antes do download e limpeza após gravação](https://github.com/mautrix/whatsapp/blob/1fc894948c033c300529bd96a282a9e7159ef543/pkg/connector/backfill.go#L106-L183).
  Referência para registrar trabalho pendente antes de baixar seu conteúdo.
