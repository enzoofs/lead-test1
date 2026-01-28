"""
Modelos de dados para o sistema de leads
"""
from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, HttpUrl


class LeadClassification(str, Enum):
    HOT = "hot"
    WARM = "warm"
    COLD = "cold"
    LOW = "low"


class LeadStatus(str, Enum):
    NEW = "novo"
    CONTACTED = "contatado"
    QUALIFIED = "qualificado"
    CONVERTED = "convertido"
    LOST = "perdido"


class SocialProfiles(BaseModel):
    """Perfis de redes sociais do lead"""
    instagram: Optional[str] = None
    instagram_followers: Optional[int] = None
    instagram_posts: Optional[int] = None
    instagram_last_post: Optional[datetime] = None

    linkedin: Optional[str] = None
    linkedin_company_id: Optional[str] = None
    linkedin_employees: Optional[int] = None

    facebook: Optional[str] = None
    twitter: Optional[str] = None
    youtube: Optional[str] = None


class GoogleMapsData(BaseModel):
    """Dados extraidos do Google Maps"""
    place_id: Optional[str] = None
    rating: Optional[float] = None
    num_reviews: Optional[int] = None
    price_level: Optional[str] = None
    hours: Optional[dict] = None
    photos_count: Optional[int] = None
    types: list[str] = Field(default_factory=list)


class Lead(BaseModel):
    """Modelo principal de Lead"""
    # Identificacao
    id: Optional[str] = None
    nome: str
    categoria: str

    # Contato
    telefone: Optional[str] = None
    email: Optional[str] = None

    # Localizacao
    endereco: Optional[str] = None
    cidade: str = "Belo Horizonte"
    estado: str = "MG"
    cep: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Online
    site: Optional[str] = None
    site_ativo: bool = False
    site_https: bool = False

    # Redes sociais
    social: SocialProfiles = Field(default_factory=SocialProfiles)

    # Google Maps
    google_maps: GoogleMapsData = Field(default_factory=GoogleMapsData)

    # Qualificacao
    score: int = 0
    classificacao: LeadClassification = LeadClassification.LOW
    status: LeadStatus = LeadStatus.NEW

    # Metadata
    data_captura: datetime = Field(default_factory=datetime.now)
    data_atualizacao: Optional[datetime] = None
    fonte: str = "google_maps"
    notas: Optional[str] = None

    # Flags de processamento
    social_enriched: bool = False
    score_calculated: bool = False
    synced_to_airtable: bool = False

    class Config:
        use_enum_values = True


class SearchQuery(BaseModel):
    """Parametros de busca"""
    query: str
    location: str = "Belo Horizonte, MG, Brasil"
    category: str
    limit: int = 20


class ScrapingResult(BaseModel):
    """Resultado de uma operacao de scraping"""
    success: bool
    leads: list[Lead] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    total_found: int = 0
    query: Optional[SearchQuery] = None
    duration_seconds: float = 0.0
