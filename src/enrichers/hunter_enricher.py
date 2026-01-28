"""
Enriquecimento de leads usando Hunter.io API

Hunter.io fornece:
- Emails corporativos
- Links para redes sociais
- Dados da empresa

Free tier: 25 buscas/mes
"""
import time
import structlog
import httpx
from typing import Optional

from config.settings import HUNTER_API_KEY, TIMEOUT_SECONDS
from src.models import Lead, SocialProfiles

logger = structlog.get_logger()


class HunterEnricher:
    """
    Enriquece leads usando Hunter.io API

    Endpoints usados:
    - Domain Search: busca emails por dominio
    - Email Finder: encontra email de pessoa especifica
    """

    BASE_URL = "https://api.hunter.io/v2"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or HUNTER_API_KEY
        if not self.api_key:
            logger.warning("HUNTER_API_KEY nao configurada")

        self.session = httpx.Client(timeout=TIMEOUT_SECONDS)

    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()

    def _extract_domain(self, url: str) -> Optional[str]:
        """Extrai dominio de uma URL"""
        if not url:
            return None

        # Remover protocolo
        domain = url.replace("https://", "").replace("http://", "")

        # Remover path
        domain = domain.split("/")[0]

        # Remover www
        domain = domain.replace("www.", "")

        return domain if domain else None

    def enrich(self, lead: Lead) -> Lead:
        """
        Enriquece lead com dados do Hunter.io

        Args:
            lead: Lead com site preenchido

        Returns:
            Lead enriquecido
        """
        if not self.api_key:
            return lead

        domain = self._extract_domain(lead.site)
        if not domain:
            return lead

        logger.info(f"Enriquecendo {lead.nome} via Hunter.io")

        try:
            # Domain Search
            data = self._domain_search(domain)

            if data:
                # Extrair email principal
                if not lead.email and data.get("emails"):
                    emails = data["emails"]
                    # Preferir emails genericos (contato@, comercial@)
                    generic_patterns = ["contato", "comercial", "info", "atendimento"]
                    for email_data in emails:
                        email = email_data.get("value", "")
                        if any(p in email.lower() for p in generic_patterns):
                            lead.email = email
                            break

                    # Fallback: primeiro email
                    if not lead.email and emails:
                        lead.email = emails[0].get("value")

                # Extrair redes sociais se ainda nao tiver
                if not lead.social.linkedin:
                    linkedin = data.get("linkedin")
                    if linkedin:
                        lead.social.linkedin = linkedin

                if not lead.social.twitter:
                    twitter = data.get("twitter")
                    if twitter:
                        lead.social.twitter = f"https://twitter.com/{twitter}"

                if not lead.social.facebook:
                    facebook = data.get("facebook")
                    if facebook:
                        lead.social.facebook = facebook

                logger.info(f"Hunter.io: email={lead.email}")

        except Exception as e:
            logger.error(f"Erro Hunter.io: {e}")

        return lead

    def _domain_search(self, domain: str) -> Optional[dict]:
        """
        Busca informacoes de um dominio

        Args:
            domain: Dominio a buscar (ex: empresa.com.br)

        Returns:
            Dados do dominio ou None
        """
        try:
            response = self.session.get(
                f"{self.BASE_URL}/domain-search",
                params={
                    "domain": domain,
                    "api_key": self.api_key,
                }
            )

            if response.status_code == 200:
                result = response.json()
                return result.get("data", {})

            elif response.status_code == 401:
                logger.error("Hunter.io: API key invalida")

            elif response.status_code == 429:
                logger.warning("Hunter.io: Rate limit atingido")

            else:
                logger.warning(f"Hunter.io: Status {response.status_code}")

        except Exception as e:
            logger.error(f"Erro na requisicao Hunter.io: {e}")

        return None

    def get_account_info(self) -> dict:
        """Retorna informacoes da conta (creditos restantes)"""
        if not self.api_key:
            return {"error": "API key nao configurada"}

        try:
            response = self.session.get(
                f"{self.BASE_URL}/account",
                params={"api_key": self.api_key}
            )

            if response.status_code == 200:
                return response.json().get("data", {})

        except Exception as e:
            logger.error(f"Erro ao buscar conta: {e}")

        return {}

    def enrich_leads(self, leads: list[Lead]) -> list[Lead]:
        """
        Enriquece lista de leads

        IMPORTANTE: Usar com cuidado devido ao limite de requisicoes
        """
        if not self.api_key:
            logger.warning("Hunter.io desativado - sem API key")
            return leads

        # Verificar creditos disponiveis
        account = self.get_account_info()
        remaining = account.get("requests", {}).get("searches", {}).get("available", 0)

        logger.info(f"Hunter.io: {remaining} buscas disponiveis")

        if remaining < len(leads):
            logger.warning(
                f"Creditos insuficientes. Disponivel: {remaining}, "
                f"Necessario: {len(leads)}"
            )

        enriched = []
        used = 0

        for lead in leads:
            if used >= remaining:
                logger.warning("Limite de creditos atingido")
                enriched.append(lead)
                continue

            if lead.site:
                enriched_lead = self.enrich(lead)
                enriched.append(enriched_lead)
                used += 1
                time.sleep(1)  # Rate limiting
            else:
                enriched.append(lead)

        return enriched
