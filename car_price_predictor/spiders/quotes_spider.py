import scrapy
import re
import asyncio
import json
import os
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

class AutosphereSpider(scrapy.Spider):
    name = 'autosphere'
    
    # 1. On utilise la liste pour l'index, mais on vide start_urls
    MAX_PAGES = 100
    
    # --- MODIFICATION URL DE PAGINATION ---
    # L'ancienne URL était: f'...&page={page}&'
    # La nouvelle structure utilise 'from=' (un offset)
    # Les logs montraient 23 fiches par page, donc on utilise 23 comme multiplicateur.
    ITEMS_PER_PAGE = 23
    page_urls = [
        f'https://www.autosphere.fr/recherche?from={page_num * 23}'
        for page_num in range(0, MAX_PAGES) # page_num 0 -> from=0, page_num 1 -> from=23, etc.
    ]
    # --- FIN MODIFICATION ---
    
    # IMPORTANT: On vide start_urls pour empêcher Scrapy de tout lancer en parallèle
    start_urls = [] 

    output_file = "autosphere_data.json"

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
        'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 60000, 
        'DOWNLOAD_TIMEOUT': 180, 
        'LOG_LEVEL': 'INFO',
        'CONCURRENT_REQUESTS': 8, 
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if os.path.exists(self.output_file):
            os.remove(self.output_file)
        with open(self.output_file, "w", encoding="utf-8") as f:
            f.write("[\n")
        self.first_item = True
        
        self.page_counters = {} 
        self.current_page_index = 0 # Commencera à l'index 0 (from=0)

    def close(self, reason):
        """Ferme proprement le JSON"""
        with open(self.output_file, "a", encoding="utf-8") as f:
            f.write("\n]")
        self.logger.info(f"✅ Données sauvegardées dans {self.output_file}")

    def save_item(self, item):
        """Sauvegarde un item JSON proprement"""
        with open(self.output_file, "a", encoding="utf-8") as f:
            if not self.first_item:
                f.write(",\n")
            json.dump(item, f, ensure_ascii=False, indent=2)
            self.first_item = False

    def normalize_key(self, text):
        """Nettoie les clés de dictionnaire"""
        if not text: return None
        text = text.lower().replace(':', '').strip()
        text = re.sub(r'[\s\u202f\xa0]+', '_', text)
        text = text.replace('é', 'e').replace('è', 'e').replace('à', 'a').replace('ô', 'o').replace('î', 'i')
        return text

    def clean_value(self, text):
        """Nettoie les valeurs extraites"""
        if text:
            return text.strip().replace('\u202f', ' ').replace('\xa0', ' ')
        return None

    def start_requests(self):
        """ 3. MODIFIÉ: Ne lance QUE la première page. """
        if self.current_page_index < len(self.page_urls):
            url = self.page_urls[self.current_page_index]
            # Log mis à jour pour refléter l'offset
            self.logger.info(f"▶️ Lancement de la Page {self.current_page_index + 1} (Offset {self.current_page_index * self.ITEMS_PER_PAGE})")
            yield scrapy.Request(
                url,
                callback=self.extract_links,
                meta={
                    "playwright": True,
                    "playwright_page_kwargs": {"wait_until": "networkidle"},
                    "playwright_include_page": True, 
                    "page_index": self.current_page_index # On passe l'index
                }
            )

    async def extract_links(self, response):
        """ 4. CORRIGÉ: Toute la logique est DANS le 'try' """
        page_index = response.meta["page_index"]
        page = response.meta.get("playwright_page") 

        if not page:
            self.logger.error(f"❌ Pas de page Playwright trouvée pour {response.url}")
            return

        try:
            # Attend que les liens des fiches soient chargés
            await page.wait_for_selector('//a[starts-with(@href, "/fiche") and @tabindex="-1"]', timeout=20000)
            final_body = await page.content()
            response = response.replace(body=final_body.encode('utf-8'))
        
            fiche_links = response.xpath('//a[starts-with(@href, "/fiche") and @tabindex="-1"]/@href').getall()
            fiche_links = list(set(fiche_links))
            
            num_fiches = len(fiche_links)
            self.logger.info(f"📄 Page {page_index + 1}: {num_fiches} fiches trouvées sur {response.url}")

            if num_fiches == 0:
                self.logger.warning(f"⚠️ Page {page_index + 1} vide. Passage à la suivante (ou fin).")
                # Si 0 fiches, on lance la page suivante manuellement
                for req in self.launch_next_page(): 
                    yield req
                return

            # 5. INITIALISATION DU COMPTEUR
            self.page_counters[page_index] = num_fiches

            for link in fiche_links:
                full_url = response.urljoin(link)
                yield scrapy.Request(
                    url=full_url,
                    callback=self.parse_fiche_technique,
                    meta={
                        "playwright": True,
                        "playwright_page_kwargs": {"wait_until": "domcontentloaded"},
                        "playwright_include_page": True,
                        "page_index": page_index # On passe l'index aux fiches
                    }
                )
        
        except Exception as e:
            self.logger.error(f"❌ Erreur Playwright ou Timeout sur la page de recherche {response.url}: {e}")
            # Si la page de recherche échoue, on tente de lancer la suivante
            for req in self.launch_next_page():
                yield req
            return
        
        finally:
            if page:
                await page.close() # Ferme la page de RECHERCHE

    async def parse_fiche_technique(self, response):
        """
        6. CORRIGÉ: Toute la logique est DANS le 'try'
        """
        page_index = response.meta["page_index"] # Récupère l'index de la page parente
        page = response.meta.get("playwright_page")
        car_data = {}

        if not page:
            self.logger.error(f"❌ Pas de page Playwright trouvée pour {response.url}")
            # On décrémente même en cas d'erreur pour ne pas bloquer la file
            for req in self.decrement_and_launch_next(page_index):
                yield req
            return

        try:
            await page.wait_for_selector("h2", timeout=12000)
            final_body = await page.content()
            response = response.replace(body=final_body.encode('utf-8'))

            # === Extraction (déplacée DANS le try) ===
            title = response.css('p[data-testid="firstParagraph"] strong::text').get()
            car_data["nom_complet_vehicule"] = self.clean_value(title) if title else "Titre non trouvé"
            
            price_raw = response.xpath('//meta[@name="product:price:amount"]/@content').get()
            if not price_raw:
                price_raw = response.xpath('//p[contains(text(),"au prix de")]/strong/text()').get()
            if price_raw:
                try:
                    car_data["prix_ttc_eur"] = int(re.sub(r'\D', '', price_raw))
                except ValueError:
                    pass

            for section in response.xpath('//h2'):
                titre_section = section.xpath('.//text()').get()
                if not titre_section: continue
                titre_section = titre_section.strip()
                div_suivant = section.xpath('./following::div[contains(@class, "grid")][1]')
                for li in div_suivant.xpath('.//li'):
                    label = li.xpath('.//span[1]//text()').get()
                    valeur = li.xpath('.//span[contains(@class,"font-semibold")]/text()').get()
                    if label and valeur:
                        cle = self.normalize_key(f"{titre_section}_{label}")
                        car_data[cle] = self.clean_value(valeur)

            car_data["url"] = response.url
            self.save_item(car_data)
            
            # 7. LOG ET DÉCOMPTE (DANS le try)
            items_restants = self.page_counters.get(page_index, 1) - 1
            self.logger.info(f"✅ Fiche de Page {page_index + 1} sauvegardée. ({items_restants} restantes sur cette page)")
            
            for req in self.decrement_and_launch_next(page_index):
                yield req
            
            yield car_data

        except Exception as e:
            self.logger.error(f"❌ Erreur Playwright ou Timeout sur {response.url}: {e}")
            
            # On décrémente même en cas d'erreur pour ne pas bloquer la file
            for req in self.decrement_and_launch_next(page_index):
                yield req
                
            return # Ne pas yield l'item
        finally:
            if page:
                await page.close() # Ferme la page de FICHE

    def decrement_and_launch_next(self, page_index):
        """
        8. Fonction clé: Décrémente et lance la page suivante si le compteur est à 0.
        """
        if page_index not in self.page_counters:
            return

        self.page_counters[page_index] -= 1
        
        if self.page_counters[page_index] == 0:
            self.logger.info(f"--- 🛑 PAGE {page_index + 1} COMPLÈTEMENT TERMINÉE ---")
            del self.page_counters[page_index] # Nettoyage
            yield from self.launch_next_page() # Lance la page suivante

    def launch_next_page(self):
        """
        9. Fonction Helper: Lance la requête pour la page suivante dans la liste.
        """
        self.current_page_index += 1 # On passe à la page suivante
        
        if self.current_page_index < len(self.page_urls):
            url = self.page_urls[self.current_page_index]
            # Log mis à jour pour refléter l'offset
            self.logger.info(f"▶️ Lancement de la Page {self.current_page_index + 1} (Offset {self.current_page_index * self.ITEMS_PER_PAGE})")
            yield scrapy.Request(
                url,
                callback=self.extract_links,
                meta={
                    "playwright": True,
                    "playwright_page_kwargs": {"wait_until": "networkidle"},
                    "playwright_include_page": True, 
                    "page_index": self.current_page_index
                }
            )
        else:
            self.logger.info("🏁 Pagination terminée. Toutes les pages ont été lancées.")

