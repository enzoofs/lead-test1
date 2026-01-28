# Sistema de Captacao e Qualificacao de Leads B2B

Sistema automatizado para captacao e qualificacao de leads B2B em Belo Horizonte, desenvolvido para a TimeLabs.

## Funcionalidades

- Scraping do Google Maps (11 categorias de negocios)
- Extracao automatica de Instagram e LinkedIn dos sites
- Enriquecimento de dados via Hunter.io (opcional)
- Sistema de scoring de leads (0-100 pontos)
- Classificacao automatica (Hot/Warm/Cold/Low)
- Sincronizacao com Airtable

## Arquitetura

```
Google Maps API (SerpAPI)
         |
         v
   Lead Extractor
         |
         v
  Social Extractor ----> Visita sites, extrai redes sociais
         |
         v
   Lead Scorer --------> Pontua e classifica
         |
         v
   Airtable Sync ------> Persiste dados
```

## Instalacao

### 1. Clone o repositorio

```bash
git clone <repo-url>
cd lead-test1
```

### 2. Crie ambiente virtual

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate  # Windows
```

### 3. Instale dependencias

```bash
pip install -r requirements.txt
```

### 4. Configure variaveis de ambiente

```bash
cp .env.example .env
# Edite .env com suas chaves de API
```

## Configuracao de APIs

### SerpAPI (Obrigatorio para modo recomendado)

1. Crie conta em https://serpapi.com/
2. Copie sua API key
3. Adicione em `.env`: `SERPAPI_KEY=sua_chave`

**Custo**: ~$50/5000 buscas

### Hunter.io (Opcional)

1. Crie conta em https://hunter.io/
2. Copie sua API key
3. Adicione em `.env`: `HUNTER_API_KEY=sua_chave`

**Custo**: 25 buscas gratis/mes

### Airtable (Obrigatorio para persistencia)

1. Crie conta em https://airtable.com/
2. Crie uma base com tabela "Leads"
3. Gere token em https://airtable.com/create/tokens
4. Adicione em `.env`:
   - `AIRTABLE_API_KEY=sua_chave`
   - `AIRTABLE_BASE_ID=appXXXXXX`

**Campos da tabela Leads**:
- Nome (Single line text)
- Categoria (Single select)
- Telefone (Phone number)
- Email (Email)
- Endereco (Single line text)
- Cidade (Single line text)
- Site (URL)
- Instagram (URL)
- LinkedIn (URL)
- Rating (Number)
- Num Reviews (Number)
- Score (Number)
- Classificacao (Single select: hot, warm, cold, low)
- Status (Single select: novo, contatado, qualificado, convertido, perdido)
- Data Captura (Date)
- Notas (Long text)

## Uso

### Execucao completa

```bash
python main.py
```

### Modo teste (5 leads, sem Airtable)

```bash
python main.py --test
```

### Categoria especifica

```bash
python main.py --category "clinica medica"
```

### Multiplas categorias

```bash
python main.py --categories "clinica medica" "academia" "pet shop"
```

### Sem SerpAPI (scraping direto)

```bash
python main.py --no-serpapi
```

### Com Hunter.io

```bash
python main.py --hunter
```

### Listar categorias

```bash
python main.py --list-categories
```

### Salvar resultados

```bash
python main.py --output resultados.json
```

## Categorias de Negocios

1. Clinica medica
2. Clinica odontologica
3. Escritorio advocacia
4. Escritorio contabilidade
5. Imobiliaria
6. Academia
7. Restaurante
8. Pet shop
9. Salao de beleza
10. Loja de roupas
11. Escola particular

## Sistema de Scoring

### Criterios (100 pontos total)

**Dados de Contato (40 pts)**
- Telefone: 10 pts
- Email: 10 pts
- Site: 10 pts
- HTTPS: 5 pts
- Site ativo: 5 pts

**Presenca Digital (30 pts)**
- Instagram: 10 pts
- Instagram ativo: 5 pts
- LinkedIn: 10 pts
- LinkedIn company: 5 pts

**Qualidade (30 pts)**
- Rating 4+: 10 pts
- 50+ reviews: 10 pts
- Horario funcionamento: 5 pts
- Categoria prioritaria: 5 pts

### Classificacao

| Score | Classificacao | Acao |
|-------|---------------|------|
| 80-100 | Hot | Contato imediato |
| 60-79 | Warm | Nurturing |
| 40-59 | Cold | Campanhas gerais |
| 0-39 | Low | Ignorar |

## Integracao com N8N

Para integrar com N8N, use o node "Execute Command":

```bash
cd /caminho/para/lead-test1 && python main.py --output /tmp/leads.json
```

Ou crie um webhook no N8N e chame o pipeline via HTTP.

## Estrutura do Projeto

```
lead-test1/
├── config/
│   └── settings.py       # Configuracoes
├── docs/
│   └── ARCHITECTURE.md   # Documentacao tecnica
├── src/
│   ├── scrapers/
│   │   ├── google_maps.py        # Scraping direto
│   │   └── google_maps_serpapi.py # Via SerpAPI
│   ├── enrichers/
│   │   ├── social_extractor.py   # Extrai redes sociais
│   │   ├── website_analyzer.py   # Analisa websites
│   │   └── hunter_enricher.py    # Enriquece via Hunter
│   ├── integrations/
│   │   └── airtable_sync.py      # Sync com Airtable
│   ├── models.py         # Modelos de dados
│   ├── scoring.py        # Logica de scoring
│   └── pipeline.py       # Pipeline orquestrador
├── tests/                # Testes
├── main.py              # Script principal
├── requirements.txt     # Dependencias
└── .env.example        # Exemplo de configuracao
```

## Custos Estimados (1000 leads/mes)

| Servico | Custo |
|---------|-------|
| SerpAPI | $50/mes |
| Hunter.io | $0-49/mes |
| Airtable | $0-20/mes |
| **Total** | **$50-120/mes** |

## Proximos Passos

1. [ ] Implementar webhook para notificar leads Hot
2. [ ] Adicionar cache para evitar re-processar leads
3. [ ] Criar dashboard de metricas
4. [ ] Integrar com CRM

## Suporte

Desenvolvido para TimeLabs - Automacao e IA

---

**Versao**: 1.0.0
