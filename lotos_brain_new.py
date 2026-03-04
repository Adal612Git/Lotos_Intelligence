
import argparse
import asyncio
import csv
import json
import logging
import os
import random
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import google.generativeai as genai
import pandas as pd
from colorama import Fore, Style, init
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ==========================
# CONFIG BASICO
# ==========================
API_KEY = os.getenv("GEMINI_API_KEY", "").strip()

ZONAS = ["Zona Chapultepec", "Providencia", "Santa Tere"]
RUBROS = ["Restaurantes", "Notarias", "Talleres Mecanicos", "Spas"]
CIUDAD = "Guadalajara"

MODEL_NAME = "gemini-2.0-flash-exp"
OUTPUT_XLSX = "Reporte_Lotos.xlsx"
PARTIAL_CSV = "Reporte_Lotos_Parcial.csv"
LOG_FILE = "lotos_execution.log"
AUDIT_FILE = "lotos_audit.jsonl"

MAX_CONCURRENT_CONTEXTS = 5
MAX_CONCURRENT_AI = 3
PARTIAL_FLUSH_EVERY = 5
MAX_RESULTS_PER_QUERY = 30

HEADLESS = True
PRECHECK_NETWORK = True
PRECHECK_GEMINI = False

MIN_DELAY_SEC = 0.4
MAX_DELAY_SEC = 1.2

MAX_REVIEW_LEN = 280
MAX_PITCH_LEN = 180

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]

init(autoreset=True)

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("lotos")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)

    return logger


LOGGER = setup_logging()


def mask_key(key: str) -> str:
    if not key:
        return ""
    if len(key) <= 8:
        return "*" * len(key)
    return f"{key[:4]}...{key[-4:]}"


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Prospecto:
    Zona: str
    Rubro: str
    Consulta: str
    Nombre: str
    Telefono: str
    Sitio_Web: str
    Tiene_Web: str
    Resena: str
    Estrategia: str = ""
    Pitch: str = ""
    Probabilidad: str = ""
    Temperatura: str = ""
    Fecha: str = ""


@dataclass
class Stats:
    total_listings: int = 0
    total_ai_calls: int = 0
    total_descartes: int = 0
    total_guardados: int = 0
    total_errores: int = 0
class AuditLogger:
    def __init__(self, run_id: str):
        self.run_id = run_id

    def event(self, name: str, payload: Optional[Dict[str, str]] = None) -> None:
        try:
            record = {
                "ts": now_utc(),
                "run_id": self.run_id,
                "event": name,
                "payload": payload or {},
            }
            with open(AUDIT_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception as exc:
            LOGGER.warning(f"Audit write failed: {exc}")


class LotosMiner:
    def __init__(self, browser, audit: AuditLogger, stats: Stats):
        self.browser = browser
        self.audit = audit
        self.stats = stats

    async def _delay(self) -> None:
        await asyncio.sleep(random.uniform(MIN_DELAY_SEC, MAX_DELAY_SEC))

    async def _accept_consent(self, page) -> None:
        selectors = [
            "button:has-text('Aceptar')",
            "button:has-text('Acepto')",
            "button:has-text('I agree')",
            "button:has-text('Accept')",
        ]
        for sel in selectors:
            try:
                btn = page.locator(sel)
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    LOGGER.info("Consent accepted")
                    self.audit.event("consent_accepted")
                    break
            except Exception as exc:
                LOGGER.warning(f"Consent click failed: {exc}")

    async def _scroll_results(self, panel) -> None:
        for _ in range(15):
            try:
                await panel.evaluate("el => { el.scrollBy(0, el.scrollHeight); }")
                await self._delay()
            except Exception as exc:
                LOGGER.warning(f"Scroll failed: {exc}")
                break
    async def _extract_listing_details(self, page) -> Dict[str, str]:
        name = ""
        phone = ""
        website = ""
        has_website = "No"
        reviews = ""

        try:
            name = (await page.locator("h1").first.inner_text(timeout=5000)).strip()
        except Exception as exc:
            LOGGER.warning(f"Name extract failed: {exc}")

        try:
            website_el = page.locator("a[aria-label*='Sitio web'], a[aria-label*='Website']")
            if await website_el.count() > 0:
                website = (await website_el.first.get_attribute("href")) or ""
                has_website = "Si" if website else "No"
        except Exception as exc:
            LOGGER.warning(f"Website extract failed: {exc}")

        try:
            phone_el = page.locator(
                "button[aria-label^='Tel\u00e9fono'], button[aria-label^='Telefono'], button[aria-label^='Phone']"
            )
            if await phone_el.count() > 0:
                aria = await phone_el.first.get_attribute("aria-label")
                phone = self._extract_phone(aria or "")
        except Exception as exc:
            LOGGER.warning(f"Phone extract failed: {exc}")

        if not phone:
            try:
                page_text = await page.locator("body").inner_text(timeout=2000)
                phone = self._extract_phone(page_text)
            except Exception as exc:
                LOGGER.warning(f"Phone fallback failed: {exc}")

        try:
            review_el = page.locator(
                "div[aria-label*='Rese\u00f1a'], div[aria-label*='Resena'], div[aria-label*='Review']"
            )
            if await review_el.count() > 0:
                reviews = (await review_el.first.inner_text()).strip()
        except Exception as exc:
            LOGGER.warning(f"Review extract failed: {exc}")

        reviews = reviews[:MAX_REVIEW_LEN]

        return {
            "Nombre": name,
            "Telefono": phone,
            "Sitio_Web": website,
            "Tiene_Web": has_website,
            "Resena": reviews,
        }

    def _extract_phone(self, text: str) -> str:
        if not text:
            return ""
        m = re.findall(r"(\+?\d[\d\s()\-]{6,})", text)
        return self._normalize_phone(m[0]) if m else ""

    def _normalize_phone(self, text: str) -> str:
        return re.sub(r"[^0-9+()\-\s]", "", text).strip()

    async def _detect_block(self, page) -> bool:
        try:
            url = page.url or ""
            if "sorry" in url:
                return True
            body = await page.locator("body").inner_text(timeout=2000)
            if "unusual traffic" in body.lower():
                return True
        except Exception:
            return False
        return False
    async def collect_listings(self, zona: str, rubro: str) -> List[Dict[str, str]]:
        query = f"{rubro} en {zona}, {CIUDAD}"
        LOGGER.info(f"Searching: {query}")
        self.audit.event("search_start", {"query": query})

        ua = random.choice(USER_AGENTS)
        context = await self.browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent=ua,
            locale="es-MX",
        )
        page = await context.new_page()

        results: List[Dict[str, str]] = []

        try:
            try:
                await page.goto("https://www.google.com/maps", wait_until="domcontentloaded")
            except Exception as exc:
                LOGGER.error(f"Page goto failed: {exc}")
                self.audit.event("page_goto_failed", {"query": query})
                self.stats.total_errores += 1
                return results

            await self._accept_consent(page)

            try:
                search_box = page.locator("input#searchboxinput")
                await search_box.fill(query)
                await page.keyboard.press("Enter")
                await self._delay()
            except Exception as exc:
                LOGGER.error(f"Search submit failed: {exc}")
                self.audit.event("search_submit_failed", {"query": query})
                self.stats.total_errores += 1
                return results

            try:
                await page.wait_for_selector("div[role='feed']", timeout=12000)
            except PlaywrightTimeoutError as exc:
                LOGGER.warning(f"Results panel timeout: {exc}")
                self.audit.event("results_timeout", {"query": query})
                return results

            if await self._detect_block(page):
                LOGGER.error("Traffic block detected. Stopping this task.")
                self.audit.event("traffic_block", {"query": query})
                self.stats.total_errores += 1
                return results

            panel = page.locator("div[role='feed']")
            await self._scroll_results(panel)

            cards = page.locator("div[role='article']")
            try:
                count = await cards.count()
            except Exception as exc:
                LOGGER.error(f"Cards count failed: {exc}")
                self.stats.total_errores += 1
                return results

            for i in range(min(count, MAX_RESULTS_PER_QUERY)):
                try:
                    card = cards.nth(i)
                    await card.click(timeout=5000)
                    await page.wait_for_timeout(1200)

                    details = await self._extract_listing_details(page)
                    if not details.get("Nombre"):
                        continue

                    details.update({"Zona": zona, "Rubro": rubro, "Consulta": query})
                    results.append(details)
                except Exception as exc:
                    LOGGER.warning(f"Card processing failed: {exc}")
                    self.stats.total_errores += 1
                    continue
        finally:
            try:
                await context.close()
            except Exception as exc:
                LOGGER.warning(f"Context close failed: {exc}")

        self.stats.total_listings += len(results)
        self.audit.event("search_complete", {"query": query, "count": str(len(results))})
        return results

class LotosPipeline:
    def __init__(self):
        self.buffer: List[Prospecto] = []
        self.buffer_lock = asyncio.Lock()
        self.partial_lock = asyncio.Lock()
        self.partial_count = 0
        self.seen: Set[str] = set()

        self._ensure_partial_header()
        self._load_seen()

    def _ensure_partial_header(self) -> None:
        try:
            with open(PARTIAL_CSV, "r", encoding="utf-8"):
                return
        except FileNotFoundError:
            pass

        headers = [
            "Zona",
            "Rubro",
            "Consulta",
            "Nombre",
            "Telefono",
            "Sitio_Web",
            "Tiene_Web",
            "Resena",
            "Estrategia",
            "Pitch",
            "Probabilidad",
            "Temperatura",
            "Fecha",
        ]
        with open(PARTIAL_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)

    def _load_seen(self) -> None:
        try:
            with open(PARTIAL_CSV, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = self._make_key(row.get("Zona", ""), row.get("Rubro", ""), row.get("Nombre", ""))
                    if key:
                        self.seen.add(key)
        except Exception:
            return

    def _make_key(self, zona: str, rubro: str, nombre: str) -> str:
        if not nombre:
            return ""
        return f"{zona.strip().lower()}|{rubro.strip().lower()}|{nombre.strip().lower()}"

    def is_seen(self, zona: str, rubro: str, nombre: str) -> bool:
        return self._make_key(zona, rubro, nombre) in self.seen

    async def add(self, prospect: Prospecto) -> None:
        async with self.buffer_lock:
            key = self._make_key(prospect.Zona, prospect.Rubro, prospect.Nombre)
            if key and key in self.seen:
                return
            if key:
                self.seen.add(key)
            self.buffer.append(prospect)
            self.partial_count += 1

        if self.partial_count % PARTIAL_FLUSH_EVERY == 0:
            await self.flush_partial()

    async def flush_partial(self) -> None:
        async with self.partial_lock:
            async with self.buffer_lock:
                to_write = list(self.buffer)
                self.buffer.clear()

            if not to_write:
                return

            with open(PARTIAL_CSV, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                for p in to_write:
                    writer.writerow([
                        p.Zona,
                        p.Rubro,
                        p.Consulta,
                        p.Nombre,
                        p.Telefono,
                        p.Sitio_Web,
                        p.Tiene_Web,
                        p.Resena,
                        p.Estrategia,
                        p.Pitch,
                        p.Probabilidad,
                        p.Temperatura,
                        p.Fecha,
                    ])
            LOGGER.info(f"Flushed {len(to_write)} rows to partial CSV")

    async def finalize_excel(self) -> None:
        try:
            df = pd.read_csv(PARTIAL_CSV)
            df.to_excel(OUTPUT_XLSX, index=False)
            LOGGER.info(f"Final Excel saved: {OUTPUT_XLSX}")
        except Exception as exc:
            LOGGER.error(f"Final Excel save failed: {exc}")

class LotosSecurity:
    @staticmethod
    def preflight_checks(audit: AuditLogger) -> bool:
        ok = True

        if not API_KEY:
            LOGGER.error("API_KEY vacia. Define GEMINI_API_KEY y vuelve a intentar.")
            audit.event("preflight_fail", {"reason": "missing_api_key"})
            return False

        try:
            with open(PARTIAL_CSV, "a", encoding="utf-8"):
                pass
            with open(LOG_FILE, "a", encoding="utf-8"):
                pass
        except Exception as exc:
            LOGGER.error(f"No se puede escribir en disco: {exc}")
            audit.event("preflight_fail", {"reason": "disk_write"})
            return False

        LOGGER.info("Preflight OK: archivos escribibles")

        if PRECHECK_NETWORK:
            ok = ok and LotosSecurity._precheck_network(audit)

        if PRECHECK_GEMINI:
            ok = ok and LotosSecurity._precheck_gemini(audit)

        return ok

    @staticmethod
    def _precheck_network(audit: AuditLogger) -> bool:
        async def _check() -> bool:
            try:
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    page = await browser.new_page()
                    await page.goto("https://www.google.com/maps", wait_until="domcontentloaded", timeout=15000)
                    await browser.close()
                return True
            except Exception as exc:
                LOGGER.error(f"Precheck network failed: {exc}")
                audit.event("preflight_fail", {"reason": "network"})
                return False

        return asyncio.run(_check())

    @staticmethod
    def _precheck_gemini(audit: AuditLogger) -> bool:
        try:
            genai.configure(api_key=API_KEY)
            model = genai.GenerativeModel(MODEL_NAME)
            resp = model.generate_content("OK")
            _ = resp.text or ""
            LOGGER.info("Precheck Gemini OK")
            return True
        except Exception as exc:
            LOGGER.error(f"Precheck Gemini failed: {exc}")
            audit.event("preflight_fail", {"reason": "gemini"})
            return False

async def worker(
    sem: asyncio.Semaphore,
    ai_sem: asyncio.Semaphore,
    miner: LotosMiner,
    brain: LotosBrain,
    pipeline: LotosPipeline,
    audit: AuditLogger,
    stats: Stats,
    zona: str,
    rubro: str,
) -> None:
    async with sem:
        try:
            listings = await miner.collect_listings(zona, rubro)
        except Exception as exc:
            LOGGER.error(f"Collect listings failed: {exc}")
            stats.total_errores += 1
            return

        for item in listings:
            try:
                if pipeline.is_seen(item.get("Zona", ""), item.get("Rubro", ""), item.get("Nombre", "")):
                    continue

                if not item.get("Telefono") and not item.get("Sitio_Web"):
                    stats.total_descartes += 1
                    prospect = Prospecto(
                        Zona=item.get("Zona", ""),
                        Rubro=item.get("Rubro", ""),
                        Consulta=item.get("Consulta", ""),
                        Nombre=item.get("Nombre", ""),
                        Telefono=item.get("Telefono", ""),
                        Sitio_Web=item.get("Sitio_Web", ""),
                        Tiene_Web=item.get("Tiene_Web", "No"),
                        Resena=item.get("Resena", ""),
                        Estrategia="",
                        Pitch="",
                        Probabilidad="",
                        Temperatura="DESCARTE",
                        Fecha=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )
                    await pipeline.add(prospect)
                    stats.total_guardados += 1
                    continue

                async with ai_sem:
                    try:
                        ai = await brain.analyze(item)
                    except Exception as exc:
                        LOGGER.error(f"AI analyze failed: {exc}")
                        stats.total_errores += 1
                        ai = {"Temperatura": "", "Estrategia": "", "Pitch": "", "Probabilidad": ""}

                prospect = Prospecto(
                    Zona=item.get("Zona", ""),
                    Rubro=item.get("Rubro", ""),
                    Consulta=item.get("Consulta", ""),
                    Nombre=item.get("Nombre", ""),
                    Telefono=item.get("Telefono", ""),
                    Sitio_Web=item.get("Sitio_Web", ""),
                    Tiene_Web=item.get("Tiene_Web", "No"),
                    Resena=item.get("Resena", ""),
                    Estrategia=ai.get("Estrategia", ""),
                    Pitch=ai.get("Pitch", ""),
                    Probabilidad=ai.get("Probabilidad", ""),
                    Temperatura=ai.get("Temperatura", ""),
                    Fecha=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
                await pipeline.add(prospect)
                stats.total_guardados += 1
            except Exception as exc:
                LOGGER.error(f"Prospect processing failed: {exc}")
                audit.event("prospect_failed", {"error": str(exc)})
                stats.total_errores += 1

def banner() -> None:
    art = [
        " _      _       _           _______          _                 ",
        "| |    (_)     | |         |__   __|        | |                ",
        "| |     _  __ _| |_ ___ ______| | ___   ___ | | ___  ___       ",
        "| |    | |/ _` | __/ _ \\______| |/ _ \\ / _ \\| |/ _ \\/ __|",
        "| |____| | (_| | ||  __/      | | (_) | (_) | |  __/\\__ \\",
        "|______|_|\\__,_|\\__\\___|      |_|\\___/ \\___/|_|\\___||___/",
    ]
    print(Fore.CYAN + "\n".join(art) + Style.RESET_ALL)
    print(Fore.YELLOW + "Lotos Technologies - Intelligence Suite" + Style.RESET_ALL)


def menu() -> str:
    print(Fore.GREEN + "\nMenu Principal:" + Style.RESET_ALL)
    print("  1) Ejecutar Miner + IA")
    print("  2) Preflight completo (diagnostico)")
    print("  3) Exportar Excel desde CSV parcial")
    print("  4) Mostrar configuracion")
    print("  5) Salir")
    choice = input(Fore.CYAN + "Selecciona una opcion (default 1): " + Style.RESET_ALL).strip()
    return choice or "1"


def show_config() -> None:
    print(Fore.MAGENTA + "\nConfiguracion activa:" + Style.RESET_ALL)
    print(f"  API_KEY: {mask_key(API_KEY)}")
    print(f"  ZONAS: {ZONAS}")
    print(f"  RUBROS: {RUBROS}")
    print(f"  CIUDAD: {CIUDAD}")
    print(f"  MAX_CONCURRENT_CONTEXTS: {MAX_CONCURRENT_CONTEXTS}")
    print(f"  MAX_CONCURRENT_AI: {MAX_CONCURRENT_AI}")
    print(f"  PARTIAL_FLUSH_EVERY: {PARTIAL_FLUSH_EVERY}")
    print(f"  MAX_RESULTS_PER_QUERY: {MAX_RESULTS_PER_QUERY}")
    print(f"  HEADLESS: {HEADLESS}")


def run_export_only() -> None:
    pipeline = LotosPipeline()
    asyncio.run(pipeline.finalize_excel())
    print(Fore.GREEN + "Excel exportado." + Style.RESET_ALL)


def run_preflight(audit: AuditLogger) -> None:
    ok = LotosSecurity.preflight_checks(audit)
    msg = "Preflight OK" if ok else "Preflight FALLIDO"
    color = Fore.GREEN if ok else Fore.RED
    print(color + msg + Style.RESET_ALL)


def build_tasks() -> List[Tuple[str, str]]:
    tasks = []
    for zona in ZONAS:
        for rubro in RUBROS:
            tasks.append((zona, rubro))
    return tasks

async def run_pipeline(audit: AuditLogger, stats: Stats) -> None:
    sem = asyncio.Semaphore(MAX_CONCURRENT_CONTEXTS)
    ai_sem = asyncio.Semaphore(MAX_CONCURRENT_AI)

    pipeline = LotosPipeline()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=["--disable-blink-features=AutomationControlled"],
        )
        miner = LotosMiner(browser, audit, stats)
        brain = LotosBrain(API_KEY, audit, stats)

        tasks = []
        for zona, rubro in build_tasks():
            tasks.append(worker(sem, ai_sem, miner, brain, pipeline, audit, stats, zona, rubro))

        LOGGER.info(f"Launching {len(tasks)} tasks")
        await asyncio.gather(*tasks, return_exceptions=True)

        try:
            await browser.close()
        except Exception as exc:
            LOGGER.warning(f"Browser close failed: {exc}")

    await pipeline.flush_partial()
    await pipeline.finalize_excel()


def summary(stats: Stats) -> None:
    print(Fore.GREEN + "\nResumen de ejecucion:" + Style.RESET_ALL)
    print(f"  Listings: {stats.total_listings}")
    print(f"  AI calls: {stats.total_ai_calls}")
    print(f"  Descartes: {stats.total_descartes}")
    print(f"  Guardados: {stats.total_guardados}")
    print(f"  Errores: {stats.total_errores}")


def main() -> None:
    banner()

    run_id = str(uuid.uuid4())
    audit = AuditLogger(run_id)
    stats = Stats()
    audit.event("run_start", {"api_key": mask_key(API_KEY)})

    parser = argparse.ArgumentParser()
    parser.add_argument("--auto", action="store_true", help="Ejecuta sin menu")
    parser.add_argument("--preflight", action="store_true", help="Ejecuta preflight y sale")
    parser.add_argument("--export-only", action="store_true", help="Exporta Excel desde CSV")
    args = parser.parse_args()

    if args.preflight:
        run_preflight(audit)
        return

    if args.export_only:
        run_export_only()
        return

    if args.auto:
        run_preflight(audit)
        asyncio.run(run_pipeline(audit, stats))
        summary(stats)
        print(Fore.GREEN + "Proceso finalizado." + Style.RESET_ALL)
        return

    choice = menu()
    if choice == "1":
        run_preflight(audit)
        asyncio.run(run_pipeline(audit, stats))
        summary(stats)
        print(Fore.GREEN + "Proceso finalizado." + Style.RESET_ALL)
    elif choice == "2":
        run_preflight(audit)
    elif choice == "3":
        run_export_only()
    elif choice == "4":
        show_config()
    else:
        print(Fore.YELLOW + "Salida solicitada." + Style.RESET_ALL)


if __name__ == "__main__":
    main()
