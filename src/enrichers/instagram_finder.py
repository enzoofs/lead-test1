"""
Buscador de Instagram via Google Search

Quando o scraping do site nao encontra o Instagram,
busca no Google: "nome do negocio" site:instagram.com
"""
import re
import time
import structlog
from typing import Optional
from difflib import SequenceMatcher

from config.settings import SERPAPI_KEY, DELAY_BETWEEN_REQUESTS

logger = structlog.get_logger()

# Tentar importar SerpAPI
try:
    from serpapi import GoogleSearch
    SERPAPI_AVAILABLE = True
except ImportError:
    SERPAPI_AVAILABLE = False
    logger.warning("SerpAPI nao instalado, InstagramFinder desativado")


class InstagramFinder:
    """
    Busca perfis do Instagram via Google Search

    Estrategia:
    1. Busca no Google: "nome do negocio" site:instagram.com
    2. Filtra resultados para encontrar o perfil correto
    3. Valida se o username parece correto
    """

    # Usernames que devem ser ignorados (agencias, genéricos, etc)
    BLACKLIST_USERNAMES = [
        "instagram", "explore", "p", "reel", "stories",
        # Agencias de marketing comuns
        "esselimarketing", "agenciadigital", "marketingdigital",
        "socialmedia", "agenciamkt", "publicidade",
    ]

    # Palavras que indicam que NAO e o perfil correto
    BLACKLIST_WORDS = [
        "marketing", "agencia", "publicidade", "midia", "social media",
        "designer", "propaganda", "assessoria",
    ]

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or SERPAPI_KEY
        if not self.api_key:
            logger.warning("SERPAPI_KEY nao configurada")
        self.enabled = SERPAPI_AVAILABLE and bool(self.api_key)

    def find(self, business_name: str, city: str = "Belo Horizonte") -> Optional[str]:
        """
        Busca o Instagram de um negocio

        Args:
            business_name: Nome do negocio
            city: Cidade para refinar busca

        Returns:
            URL do Instagram ou None
        """
        if not self.enabled:
            return None

        # Limpar nome do negocio
        clean_name = self._clean_business_name(business_name)

        # Montar query de busca
        query = f'"{clean_name}" {city} site:instagram.com'

        logger.info(f"Buscando Instagram: {query}")

        try:
            time.sleep(DELAY_BETWEEN_REQUESTS)

            params = {
                "engine": "google",
                "q": query,
                "num": 5,
                "hl": "pt-br",
                "gl": "br",
                "api_key": self.api_key,
            }

            search = GoogleSearch(params)
            results = search.get_dict()

            organic = results.get("organic_results", [])

            for result in organic:
                link = result.get("link", "")
                title = result.get("title", "")

                # Verificar se e um perfil do Instagram
                if "instagram.com" in link.lower():
                    username = self._extract_username(link)

                    if username and self._is_valid_profile(username, clean_name, title):
                        instagram_url = f"https://instagram.com/{username}"
                        logger.info(f"Instagram encontrado: {instagram_url}")
                        return instagram_url

        except Exception as e:
            logger.error(f"Erro ao buscar Instagram: {e}")

        return None

    def _clean_business_name(self, name: str) -> str:
        """Remove sufixos comuns e limpa o nome"""
        # Remover sufixos juridicos
        suffixes = [
            "ltda", "me", "eireli", "s/a", "s.a.", "ss",
            "- belo horizonte", "- bh", "bh", "mg",
        ]

        clean = name.lower()
        for suffix in suffixes:
            clean = clean.replace(suffix, "")

        # Remover caracteres especiais
        clean = re.sub(r'[^\w\s]', ' ', clean)

        # Remover espacos extras
        clean = ' '.join(clean.split())

        return clean.strip()

    def _extract_username(self, url: str) -> Optional[str]:
        """Extrai username de uma URL do Instagram"""
        patterns = [
            r"instagram\.com/([a-zA-Z0-9_.]+)/?",
        ]

        for pattern in patterns:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                username = match.group(1).lower()
                # Ignorar paths que nao sao usernames
                if username not in ["p", "reel", "stories", "explore", "accounts"]:
                    return username

        return None

    def _is_valid_profile(self, username: str, business_name: str, title: str) -> bool:
        """
        Valida se o perfil encontrado e realmente do negocio

        Verifica:
        1. Username nao esta na blacklist
        2. Titulo nao contem palavras da blacklist
        3. Similaridade entre username e nome do negocio
        """
        username_lower = username.lower()
        title_lower = title.lower()
        name_lower = business_name.lower()

        # 1. Verificar blacklist de usernames
        if username_lower in self.BLACKLIST_USERNAMES:
            logger.debug(f"Username {username} na blacklist")
            return False

        # 2. Verificar blacklist de palavras no titulo
        for word in self.BLACKLIST_WORDS:
            if word in title_lower and word not in name_lower:
                logger.debug(f"Titulo contem '{word}' que nao esta no nome do negocio")
                return False

        # 3. Verificar similaridade
        # Remover caracteres especiais para comparar
        clean_username = re.sub(r'[^a-z0-9]', '', username_lower)
        clean_name = re.sub(r'[^a-z0-9]', '', name_lower)

        # Calcular similaridade
        similarity = SequenceMatcher(None, clean_username, clean_name).ratio()

        # Se o username contem parte significativa do nome, aceitar
        name_words = name_lower.split()
        for word in name_words:
            if len(word) >= 4 and word in clean_username:
                logger.debug(f"Username {username} contem palavra '{word}' do nome")
                return True

        # Se similaridade for alta, aceitar
        if similarity >= 0.4:
            logger.debug(f"Similaridade {similarity:.2f} entre {username} e {business_name}")
            return True

        # Se o titulo do resultado contem o nome do negocio, aceitar
        for word in name_words:
            if len(word) >= 4 and word in title_lower:
                return True

        logger.debug(f"Username {username} nao parece ser de {business_name}")
        return False

    def enrich_lead(self, lead) -> None:
        """
        Enriquece um lead com Instagram se ainda nao tiver

        Args:
            lead: Objeto Lead para enriquecer (modificado in-place)
        """
        if not self.enabled:
            return

        # So buscar se ainda nao tem Instagram
        if lead.social.instagram:
            return

        instagram_url = self.find(lead.nome, lead.cidade)

        if instagram_url:
            lead.social.instagram = instagram_url

    def enrich_leads(self, leads: list) -> list:
        """
        Enriquece lista de leads com Instagram

        Args:
            leads: Lista de leads

        Returns:
            Lista de leads (mesma referencia, modificados in-place)
        """
        if not self.enabled:
            logger.warning("InstagramFinder desativado")
            return leads

        count_before = sum(1 for l in leads if l.social.instagram)

        for i, lead in enumerate(leads, 1):
            if not lead.social.instagram:
                logger.info(f"Buscando Instagram {i}/{len(leads)}: {lead.nome}")
                self.enrich_lead(lead)
                time.sleep(1)  # Rate limiting

        count_after = sum(1 for l in leads if l.social.instagram)
        found = count_after - count_before

        logger.info(f"InstagramFinder: {found} novos perfis encontrados")

        return leads
