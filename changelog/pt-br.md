# Changelog - Português (Brasil)

Todas as alterações notáveis neste projeto serão documentadas neste arquivo.

---

### [v1.1.0] Operações Atômicas (10/01/2026)

- **Feat:** utilizando `BRPopLPush` (atômico) para busca de jobs, prevenindo race conditions.
- **Feat:** Implementação de atualização atômica de status de conclusão de jobs.

### [v1.0.0] Lidando com concorrencia de workers (versão mínima) (03/01/2026)

- **Refactor:** Lidando com concorrencia de workers (versão mínima).


### [v0.1.7] Correção na Configuração de CI/Release (02/01/2026)

- **Fix:** Correção de indentação no arquivo `.goreleaser.yaml`.

### [v0.1.6] / [v0.1.5] Otimização do Pipeline de Build (02/01/2026)

- **Fix:** Simplificação do passo do GoReleaser removendo argumentos desnecessários.

### [v0.1.4] Suporte para Distribuição Multi-plataforma (02/01/2026)

- **Feat:** Adição da configuração inicial do `.goreleaser.yaml`.

### [v0.1.3] Padronização do Namespace do Pacote (02/01/2026)

- **Fix:** Atualização do nome do pacote de `main` para `oncamq` no `worker.go`.

### [v0.1.2] Ajuste de Visibilidade de Escopo (02/01/2026)

- **Fix:** Mudança de pacote de `oncamq` para `main`.

### [v0.1.1] Resolução de Conflitos de Importação e Workflow (02/01/2026)

- **CI:** Correção de conflito de pacote/programa no caminho de importação.
- **Fix:** Atualização da mensagem de versão de release no workflow do GitHub Actions.

### [v0.1.0] Implementação Base e Padrões Idiomáticos (Gopher Way) (02/01/2026)

- **Feat:** Implementação de padrões idiomáticos de worker Go (remoção de estado global, contexto explícito).
- **Feat:** Ações de sucesso: gerenciamento de filas completas, valores de retorno e tentativas.
- **Feat:** Integração com GitHub Actions para publicação automática.
- **Docs:** Documentação expandida com guia de contribuição e exemplos realistas.
- **Chore:** Renomeação do módulo de `go-bullmq-consumer` para `oncamq`.
