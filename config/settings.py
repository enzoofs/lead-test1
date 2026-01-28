"""
Configuracoes do sistema de leads B2B - TimeLabs
"""
import os
from dotenv import load_dotenv

load_dotenv()


# API Keys
SERPAPI_KEY = os.getenv("SERPAPI_KEY", "")
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY", "")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Leads")


# Busca - Configuracoes
SEARCH_LOCATION = "Belo Horizonte, MG, Brasil"
SEARCH_LANGUAGE = "pt-br"
SEARCH_COUNTRY = "br"


# Tipos de negocios para buscar
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
    "escola particular",
]


# Rate Limiting
REQUESTS_PER_MINUTE = 10
DELAY_BETWEEN_REQUESTS = 2  # segundos


# Scraping
MAX_RETRIES = 3
TIMEOUT_SECONDS = 30
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


# Scoring - Pesos
SCORING_WEIGHTS = {
    # Dados de contato (40 pontos)
    "tem_telefone": 10,
    "tem_email": 10,
    "tem_site": 10,
    "site_com_https": 5,
    "site_ativo": 5,

    # Presenca digital (30 pontos)
    "tem_instagram": 10,
    "instagram_ativo": 5,
    "tem_linkedin": 10,
    "linkedin_company_page": 5,

    # Indicadores de qualidade (30 pontos)
    "rating_alto": 10,           # 4+ estrelas
    "muitas_reviews": 10,        # 50+ reviews
    "horario_funcionamento": 5,
    "categoria_relevante": 5,
}


# Classificacao de leads
LEAD_CLASSIFICATION = {
    "hot": (80, 100),
    "warm": (60, 79),
    "cold": (40, 59),
    "low": (0, 39),
}


# Categorias prioritarias para TimeLabs (automacao/IA)
# Leads nessas categorias recebem bonus no score
PRIORITY_CATEGORIES = [
    "clinica medica",
    "clinica odontologica",
    "escritorio advocacia",
    "escritorio contabilidade",
    "imobiliaria",
]
PRIORITY_BONUS = 5
