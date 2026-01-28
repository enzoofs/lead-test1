# Sistema de Captacao e Qualificacao de Leads B2B - TimeLabs

## 1. Analise: N8N vs Alternativas

### N8N - Limitacoes Identificadas

| Problema | Impacto |
|----------|---------|
| Appify instavel para scraping massivo | Muitos erros, rate limiting |
| Dificuldade com JavaScript dinamico | Nao extrai redes sociais corretamente |
| Fluxos complexos ficam dificeis de debugar | Manutencao complicada |
| Sem controle fino sobre retries/backoff | Falhas em cascata |

### Recomendacao: Python + Orquestracao Hibrida

**Por que Python?**
- Controle total sobre scraping (Playwright, Selenium)
- Bibliotecas robustas para APIs
- Facil integracao com Airtable
- Melhor tratamento de erros e retries
- Pode ser chamado pelo N8N se necessario

**Arquitetura Hibrida Sugerida:**
```
N8N (orquestracao/agendamento) --> Python Scripts (execucao) --> Airtable
```

## 2. Arquitetura do Sistema

```
+------------------+     +-------------------+     +------------------+
|                  |     |                   |     |                  |
|  Google Maps     |---->|  Lead Extractor   |---->|  Social Media    |
|  Scraper         |     |  (consolidacao)   |     |  Enricher        |
|                  |     |                   |     |                  |
+------------------+     +-------------------+     +------------------+
                                   |
                                   v
                         +-------------------+
                         |                   |
                         |  Lead Scorer      |
                         |  (qualificacao)   |
                         |                   |
                         +-------------------+
                                   |
                                   v
                         +-------------------+
                         |                   |
                         |  Airtable Sync    |
                         |  (persistencia)   |
                         |                   |
                         +-------------------+
```

## 3. APIs e Ferramentas Recomendadas

### Scraping Google Maps

| Opcao | Preco | Confiabilidade | Recomendacao |
|-------|-------|----------------|--------------|
| **SerpAPI** | $50/5000 buscas | Alta | Melhor custo-beneficio |
| Outscraper | $2.50/1000 | Alta | Alternativa solida |
| Apify (Google Maps) | Variavel | Media | Atual - muitos erros |
| Scraping direto | Gratis | Baixa | Risco de bloqueio |

### Extracao de Redes Sociais

| Ferramenta | Para | Custo |
|------------|------|-------|
| **Clearbit** | LinkedIn, social profiles | Free tier disponivel |
| **Hunter.io** | Email + social links | 25 buscas gratis/mes |
| **RocketReach** | LinkedIn, emails | Pago |
| **Scraping site** | Instagram do site | Gratis (parsing HTML) |

### Estrategia Recomendada (Custo-Efetiva)

1. **Google Maps**: SerpAPI ou Outscraper
2. **Redes Sociais**:
   - Primeiro: Extrair do proprio site (gratis)
   - Segundo: Hunter.io para enriquecer
   - Terceiro: Clearbit para LinkedIn

## 4. Logica de Scoring de Leads

### Criterios de Pontuacao (0-100)

```python
SCORING_CRITERIA = {
    # Dados de contato (40 pontos)
    "tem_telefone": 10,
    "tem_email": 10,
    "tem_site": 10,
    "site_com_https": 5,
    "site_ativo": 5,

    # Presenca digital (30 pontos)
    "tem_instagram": 10,
    "instagram_ativo": 5,      # posts recentes
    "tem_linkedin": 10,
    "linkedin_company_page": 5,

    # Indicadores de qualidade (30 pontos)
    "rating_google": 10,       # 4+ estrelas = 10pts
    "quantidade_reviews": 10,  # 50+ = 10pts
    "horario_funcionamento": 5,
    "categoria_match": 5,      # match com ICP
}
```

### Classificacao Final

| Score | Classificacao | Acao Recomendada |
|-------|---------------|------------------|
| 80-100 | Hot Lead | Contato imediato |
| 60-79 | Warm Lead | Nurturing ativo |
| 40-59 | Cold Lead | Campanhas gerais |
| 0-39 | Low Priority | Ignorar ou reprocessar |

## 5. Tipos de Negocios (11 categorias BH)

```python
BUSINESS_TYPES = [
    "clinica medica",
    "clinica odontologica",
    "escritorio advocacia",
    "escritorio contabilidade",
    "imobiliaria",
    "academia",
    "restaurante",
    "pet shop",
    "salao de beleza",
    "loja de roupas",
    "escola particular"
]
```

## 6. Estrutura de Dados - Airtable

### Tabela: Leads

| Campo | Tipo | Descricao |
|-------|------|-----------|
| id | Auto | ID unico |
| nome | Text | Nome do negocio |
| categoria | Single Select | Tipo de negocio |
| telefone | Phone | Telefone principal |
| email | Email | Email extraido |
| endereco | Text | Endereco completo |
| cidade | Text | Cidade |
| site | URL | Website |
| instagram | URL | Perfil Instagram |
| linkedin | URL | Pagina LinkedIn |
| rating | Number | Avaliacao Google |
| num_reviews | Number | Qtd avaliacoes |
| score | Number | Score calculado |
| classificacao | Single Select | Hot/Warm/Cold/Low |
| status | Single Select | Novo/Contatado/Convertido |
| data_captura | Date | Data de extracao |
| notas | Long Text | Observacoes |

## 7. Fluxo de Execucao

```
1. [DIARIO] Scraping Google Maps por categoria
      |
      v
2. [PARA CADA LEAD] Extrai dados basicos
      |
      v
3. [PARA CADA LEAD] Visita site e extrai redes sociais
      |
      v
4. [OPCIONAL] Enriquece com Hunter.io/Clearbit
      |
      v
5. [PARA CADA LEAD] Calcula score
      |
      v
6. [BATCH] Sincroniza com Airtable
      |
      v
7. [WEBHOOK] Notifica leads Hot no Slack/Email
```

## 8. Estimativa de Custos

### Cenario: 1000 leads/mes

| Servico | Custo Estimado |
|---------|----------------|
| SerpAPI | $50/mes |
| Hunter.io | $0-49/mes |
| Airtable | $0-20/mes |
| Servidor (opcional) | $5-10/mes |
| **Total** | **$55-130/mes** |

## 9. Proximos Passos

1. Configurar ambiente Python
2. Obter API keys (SerpAPI, Hunter.io, Airtable)
3. Executar primeiro batch de testes
4. Ajustar scoring baseado em resultados
5. Integrar com workflow comercial
