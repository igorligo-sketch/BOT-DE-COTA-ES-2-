# BOT-DE-COTA-ES-2-
# -*- coding: utf-8 -*-
"""
Bot de cotações para WhatsApp Web - VERSÃO COMPLETA (indicador CEPEA + BRL only)
Funcionalidades:
- Coleta cotações das páginas de INDICADOR do CEPEA (mantendo a mesma fonte)
- Extrai APENAS valores em R$ (ignora US$)
- Sanity check por commodity + fallback regex BRL
- Envio automático pelo WhatsApp Web (com contexto persistente)
- Envio imediato (--send-now), agendado (--schedule) e só coleta (--collect)
"""

import asyncio
import platform
import sys
import re
import json
import threading
import signal
import time
import schedule
import warnings
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

# ---- Loop do Windows: evita "I/O operation on closed pipe" no Py 3.13
if platform.system() == "Windows":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass

# Suprime warnings do Playwright/asyncio
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)
logging.getLogger("playwright").setLevel(logging.ERROR)
logging.getLogger("asyncio").setLevel(logging.ERROR)

try:
    from zoneinfo import ZoneInfo
except ImportError:
    import pytz
    class ZoneInfo:
        def __init__(self, zone):
            self.zone = pytz.timezone(zone)

import yaml
import httpx
from selectolax.parser import HTMLParser
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# =========================
# CONFIG / CONSTANTES
# =========================

TZ = ZoneInfo("America/Belem")
CONFIG_PATH = Path("config.yaml")
USER_DATA_DIR = Path("wa_profile")
CACHE_PATH = Path("cache_cotacoes.json")
LOG_PATH = Path("debug_log.txt")

shutdown_flag = threading.Event()

GLOBAL_TIMEOUT = 180  # s (coleta)
WHATSAPP_TIMEOUT = 180  # s (envio)

def signal_handler(signum, frame):
    print("\n🛑 Encerrando processo...")
    shutdown_flag.set()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Mantemos as páginas de INDICADOR
FONTES_DADOS = {
    "cepea_principal": {
        "soja": "https://www.cepea.org.br/br/indicador/soja.aspx",
        "milho": "https://www.cepea.org.br/br/indicador/milho.aspx",
        "boi":  "https://www.cepea.org.br/br/indicador/boi-gordo.aspx",
    }
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.cepea.org.br/",
}

def log_debug(msg: str):
    ts = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        def _w():
            with open(LOG_PATH, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        th = threading.Thread(target=_w, daemon=True)
        th.start()
        th.join(timeout=2)
    except:
        pass

# =========================
# HELPERS (BRL only + sanity)
# =========================

BRL_REGEXES = [
    r"R\$\s*(\d{1,3}(?:\.\d{3})*,\d{2})",  # R$ 123.456,78
    r"R\$\s*(\d{2,4},\d{2})",              # R$ 1234,56
]

def _to_float_br(s: Any) -> Optional[float]:
    if not isinstance(s, str) or not s.strip():
        return None
    s_clean = re.sub(r"[^\d,\.]", "", s.strip())
    if not s_clean:
        return None
    if "," in s_clean and "." in s_clean:
        s_clean = s_clean.replace(".", "").replace(",", ".")
    elif "," in s_clean:
        parts = s_clean.split(",")
        if len(parts) == 2 and len(parts[1]) <= 2:
            s_clean = s_clean.replace(",", ".")
        else:
            s_clean = s_clean.replace(",", "")
    try:
        v = float(s_clean)
        # limite amplo pra BRL (evita capturar US$ muito baixos)
        if 5 <= v <= 2000:
            return v
    except:
        pass
    return None

def extract_brl_only(text: str) -> Optional[float]:
    if not text:
        return None
    if "US$" in text or "USD" in text.upper():
        return None
    for pat in BRL_REGEXES:
        m = re.search(pat, text)
        if m:
            val = _to_float_br(m.group(1))
            if val is not None:
                return val
    return None

def sanitize_by_commodity(nome: str, valor: Optional[float]) -> bool:
    if valor is None:
        return False
    n = nome.lower()
    if "soja" in n:
        return 50 <= valor <= 300   # R$/saca
    if "milho" in n:
        return 20 <= valor <= 150   # R$/saca
    if "boi" in n:
        return 150 <= valor <= 400  # R$/arroba
    return True

# =========================
# PARSER (páginas de INDICADOR, BRL only)
# =========================

def parse_cepea_page(html: str) -> Optional[Dict[str, Any]]:
    """Parser para páginas de INDICADOR do CEPEA, extraindo APENAS valores em R$."""
    if shutdown_flag.is_set() or not html:
        return None
    try:
        # 1) Tenta por TABELA: coluna com "R$" (ignora cabeçalho com US$)
        doc = HTMLParser(html)
        tables = doc.css("table")
        for table in tables:
            headers = [th.text(strip=True) for th in table.css("thead tr th")]
            if not headers:
                first = table.css_first("tr")
                if first:
                    headers = [c.text(strip=True) for c in first.css("th,td")]

            def find_idx(headers, exact, contains=None):
                try:
                    return headers.index(exact)
                except ValueError:
                    if contains:
                        for i, h in enumerate(headers):
                            if contains in h:
                                return i
                return None

            idx_data = find_idx(headers, "Data", "Data")

            idx_val = None
            for i, h in enumerate(headers):
                if "R$" in h and "US$" not in h:
                    idx_val = i
                    break
            if idx_data is None or idx_val is None:
                continue

            rows = table.css("tbody tr") or table.css("tr")[1:]
            for tr in reversed(rows):
                cols = [td.text(strip=True) for td in tr.css("td")]
                if len(cols) <= max(idx_data, idx_val):
                    continue
                data_txt = cols[idx_data]
                val_txt = cols[idx_val]
                if "US$" in val_txt:
                    continue
                val = extract_brl_only(val_txt) or _to_float_br(val_txt)
                if val and re.match(r"\d{1,2}/\d{1,2}/\d{4}", data_txt):
                    return {"data": data_txt, "valor": val, "fonte_html": "table_brl"}

        # 2) Tenta por blocos de texto (somente padrões em R$)
        soup = BeautifulSoup(html, "html.parser")
        candidates = soup.find_all(["div", "span", "p", "td", "li"])
        for el in candidates[:400]:
            txt = el.get_text(" ", strip=True)
            if "US$" in txt or "USD" in txt.upper():
                continue
            val = extract_brl_only(txt)
            if val:
                # tenta achar data próxima
                d_txt = None
                cur = el
                for _ in range(3):
                    if not cur:
                        break
                    ctx = cur.get_text(" ", strip=True)
                    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", ctx)
                    if m:
                        d_txt = m.group(1)
                        break
                    cur = cur.parent
                if not d_txt:
                    d_txt = datetime.now(TZ).strftime("%d/%m/%Y")
                return {"data": d_txt, "valor": val, "fonte_html": "text_brl"}

        # 3) Fallback: regex global BRL no texto inteiro
        txt_all = soup.get_text(" ", strip=True)
        for pat in BRL_REGEXES:
            m = re.search(pat, txt_all)
            if m:
                val = _to_float_br(m.group(1))
                if val:
                    return {"data": datetime.now(TZ).strftime("%d/%m/%Y"), "valor": val, "fonte_html": "regex_brl"}

        return None
    except Exception as e:
        log_debug(f"Erro no parsing (indicador): {e}")
        return None

# =========================
# FETCHERS (HTTP + Playwright)
# =========================

async def fetch_with_timeout(url: str, timeout_sec: int = 30) -> Optional[str]:
    try:
        async with asyncio.timeout(timeout_sec):
            async with httpx.AsyncClient(
                timeout=timeout_sec, follow_redirects=True, headers=HEADERS
            ) as client:
                r = await client.get(url)
                r.raise_for_status()
                return r.text
    except Exception as e:
        log_debug(f"HTTP erro {url}: {e}")
        return None

async def fetch_playwright_with_timeout(url: str, timeout_sec: int = 60) -> Optional[str]:
    pw = None
    ctx = None
    try:
        async with asyncio.timeout(timeout_sec):
            pw = await async_playwright().start()
            ctx = await pw.chromium.launch_persistent_context(
                user_data_dir=str(USER_DATA_DIR / "cepea_profile"),
                headless=False,
                ignore_https_errors=True,
                args=["--disable-blink-features=AutomationControlled", "--no-default-browser-check"],
                locale="pt-BR",
                timezone_id="America/Belem",
            )
            await ctx.set_extra_http_headers(HEADERS)
            page = await ctx.new_page()

            # tenta diferentes estados de carregamento
            for state in ("domcontentloaded", "load", "networkidle"):
                try:
                    await page.goto(url, wait_until=state, timeout=120_000)
                except Exception:
                    continue

                # aceitar cookies comuns
                for sel in [
                    "#onetrust-accept-btn-handler",
                    "button:has-text('Aceitar')",
                    "text=Aceitar",
                    "text=OK",
                    "text=Concordo",
                    "text=Accept",
                ]:
                    try:
                        el = page.locator(sel).first
                        if await el.count():
                            await el.click(timeout=1500)
                            await page.wait_for_timeout(300)
                            break
                    except:
                        pass

                for _ in range(3):
                    await page.mouse.wheel(0, 1000)
                    await page.wait_for_timeout(250)

                html = await page.content()
                if html:
                    return html
            return None
    finally:
        try:
            if ctx: await ctx.close()
        except:
            pass
        try:
            if pw: await pw.stop()
        except:
            pass

# =========================
# COLETA (com sanity + fallback BRL)
# =========================

def load_cache() -> Dict[str, Any]:
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except:
            return {}
    return {}

def save_cache_safe(items: List[Dict[str, Any]]) -> None:
    if not items:
        return
    def _save():
        try:
            cache = load_cache()
            for it in items:
                nome = it.get("nome")
                valor = it.get("valor")
                if nome and isinstance(valor, (int, float)):
                    cache[nome] = {
                        "valor": valor,
                        "fonte": it.get("fonte", ""),
                        "data_cache": datetime.now(TZ).strftime("%d/%m/%Y %H:%M"),
                        "metodo": it.get("metodo", ""),
                    }
            CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
            log_debug("✅ Cache salvo")
        except Exception as e:
            log_debug(f"❌ Erro salvando cache: {e}")
    th = threading.Thread(target=_save, daemon=True)
    th.start()
    th.join(timeout=5)

def get_cached_item(nome: str) -> Optional[Dict[str, Any]]:
    cache = load_cache()
    data = cache.get(nome)
    if not data:
        return None
    return {
        "nome": nome,
        "valor": data.get("valor"),
        "fonte": f"{data.get('fonte','')} • (cache {data.get('data_cache','')})",
        "from_cache": True,
    }

async def collect_commodity_fast(nome: str, commodity_key: str) -> Optional[Dict[str, Any]]:
    if commodity_key not in FONTES_DADOS["cepea_principal"]:
        return None
    url = FONTES_DADOS["cepea_principal"][commodity_key]

    # 1) HTTP primeiro
    html = await fetch_with_timeout(url, 25)
    if html:
        result = parse_cepea_page(html)
        if result:
            # sanity + fallback regex global BRL se reprovar
            if not sanitize_by_commodity(nome, result["valor"]):
                txt = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
                v2 = extract_brl_only(txt)
                if v2 and sanitize_by_commodity(nome, v2):
                    result["valor"] = v2
                    result["fonte_html"] += "_fallback"
                else:
                    result = None
        if result:
            return {
                "nome": nome,
                "valor": result["valor"],
                "fonte": f"CEPEA/HTTP • {result.get('data','')}",
                "from_cache": False,
                "metodo": "http_" + result.get("fonte_html", "brl"),
            }

    # 2) Fallback Playwright
    html = await fetch_playwright_with_timeout(url, 60)
    if html:
        result = parse_cepea_page(html)
        if result:
            if not sanitize_by_commodity(nome, result["valor"]):
                txt = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
                v2 = extract_brl_only(txt)
                if v2 and sanitize_by_commodity(nome, v2):
                    result["valor"] = v2
                    result["fonte_html"] += "_fallback"
                else:
                    result = None
        if result:
            return {
                "nome": nome,
                "valor": result["valor"],
                "fonte": f"CEPEA/PLAYWRIGHT • {result.get('data','')}",
                "from_cache": False,
                "metodo": "playwright_" + result.get("fonte_html", "brl"),
            }

    return None

async def get_quotes_with_timeout() -> List[Dict[str, Any]]:
    try:
        async with asyncio.timeout(GLOBAL_TIMEOUT):
            return await get_quotes_internal()
    except asyncio.TimeoutError:
        log_debug(f"⏰ TIMEOUT GLOBAL ({GLOBAL_TIMEOUT}s) - usando cache")
        return get_fallback_from_cache()

async def get_quotes_internal() -> List[Dict[str, Any]]:
    log_debug("🚀 Iniciando coleta de cotações")
    commodities = {
        "Soja (sc)": "soja",
        "Milho (sc)": "milho",
        "Boi gordo (@)": "boi",
    }
    results: List[Dict[str, Any]] = []

    for nome, key in commodities.items():
        if shutdown_flag.is_set():
            break
        try:
            async with asyncio.timeout(60):
                r = await collect_commodity_fast(nome, key)
                if r:
                    results.append(r)
                    log_debug(f"✅ {nome}: R$ {r['valor']:.2f}")
                else:
                    cached = get_cached_item(nome)
                    if cached:
                        results.append(cached)
                        log_debug(f"📦 {nome}: cache usado")
                    else:
                        log_debug(f"❌ {nome}: falhou completamente")
        except asyncio.TimeoutError:
            log_debug(f"⏰ {nome}: timeout individual")
            cached = get_cached_item(nome)
            if cached:
                results.append(cached)

    new_items = [x for x in results if not x.get("from_cache")]
    if new_items:
        save_cache_safe(new_items)
    return results

def get_fallback_from_cache() -> List[Dict[str, Any]]:
    out = []
    for nome in ["Soja (sc)", "Milho (sc)", "Boi gordo (@)"]:
        out.append(get_cached_item(nome) or {"nome": nome, "valor": None, "fonte": "Sem dados", "from_cache": False})
    return out

# =========================
# WHATSAPP (robusto)
# =========================

async def open_whatsapp_web() -> Optional[tuple]:
    pw = ctx = page = None
    try:
        USER_DATA_DIR.mkdir(exist_ok=True)
        pw = await async_playwright().start()
        ctx = await pw.chromium.launch_persistent_context(
            user_data_dir=str(USER_DATA_DIR),
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-sandbox",
            ],
            viewport={"width": 1366, "height": 850},
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        await page.goto("https://web.whatsapp.com", timeout=120_000, wait_until="domcontentloaded")
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=60_000)
            await page.wait_for_load_state("networkidle", timeout=60_000)
        except:
            pass

        # QR?
        try:
            qr = page.locator("[data-ref]").first
            if await qr.is_visible(timeout=3_000):
                print("📱 Escaneie o QR no celular (tempo máx. 180s)…")
                await page.wait_for_selector("[data-ref]", state="hidden", timeout=180_000)
        except:
            pass

        # Confirmar UI pronta com alguns seletores alternativos
        for sel in [
            "[data-testid='chat-list']",
            "[data-testid='conversation-compose-box-input']",
            "div[contenteditable='true']",
            "div[role='textbox']",
        ]:
            try:
                await page.wait_for_selector(sel, timeout=30_000)
                break
            except:
                continue

        await page.wait_for_timeout(1000)
        return pw, ctx, page
    except Exception as e:
        log_debug(f"❌ Erro abrindo WhatsApp: {e}")
        try:
            await page.screenshot(path="debug_wa_exception.png", full_page=True)
            html = await page.content()
            Path("debug_wa_exception.html").write_text(html, encoding="utf-8")
        except:
            pass
        try:
            if ctx: await ctx.close()
        except: pass
        try:
            if pw: await pw.stop()
        except: pass
        return None

async def find_and_select_chat(page, chat_name: str) -> bool:
    try:
        # Ctrl+K para abrir busca global (se existir)
        try:
            await page.keyboard.press("Control+K")
            await page.wait_for_timeout(400)
        except:
            pass

        search_candidates = [
            "[data-testid='chat-list-search'] input",
            "[data-testid='chat-list-search']",
            "input[aria-label*='Pesquisar']",
            "div[contenteditable='true'][data-tab]",
        ]
        search = None
        for sel in search_candidates:
            loc = page.locator(sel).first
            try:
                if await loc.count():
                    await loc.click(timeout=5_000)
                    search = loc
                    break
            except:
                continue
        if search is None:
            generic = page.locator("input[type='text']").first
            if await generic.count():
                await generic.click()
                search = generic
        if search is None:
            log_debug("❌ Não achei a caixa de pesquisa.")
            return False

        await search.fill("")
        await page.wait_for_timeout(200)
        await search.fill(chat_name)
        await page.wait_for_timeout(1200)

        # Preferir o title exato
        title_locator = page.locator(f"//span[@title='{chat_name}']").first
        if await title_locator.count():
            await title_locator.click()
            await page.wait_for_timeout(700)
            return True

        # Fallback: varrer cards
        cards = page.locator("[data-testid='cell-frame-container']")
        count = await cards.count()
        for i in range(min(count, 12)):
            el = cards.nth(i)
            try:
                txt = (await el.inner_text()).strip()
                if chat_name.lower() in txt.lower():
                    await el.click()
                    await page.wait_for_timeout(700)
                    return True
            except:
                continue

        log_debug(f"❌ Chat não encontrado: {chat_name}")
        return False
    except Exception as e:
        log_debug(f"❌ Erro procurando chat: {e}")
        try:
            await page.screenshot(path="debug_wa_find_chat.png", full_page=True)
        except:
            pass
        return False

async def send_message_whatsapp(page, message: str) -> bool:
    try:
        box_candidates = [
            "[data-testid='conversation-compose-box-input']",
            "div[aria-label='Mensagem']",
            "div[contenteditable='true'][data-tab]",
            "div[contenteditable='true']",
            "div[role='textbox']",
        ]
        box = None
        for sel in box_candidates:
            loc = page.locator(sel).last
            if await loc.count():
                try:
                    await loc.wait_for(state="visible", timeout=10_000)
                    box = loc
                    break
                except:
                    continue
        if box is None:
            log_debug("❌ Não achei a caixa de mensagem.")
            try:
                await page.screenshot(path="debug_wa_no_box.png", full_page=True)
            except:
                pass
            return False

        await box.click()
        await page.wait_for_timeout(150)

        # digita com quebras (Shift+Enter)
        for line in message.split("\n"):
            await page.keyboard.type(line)
            await page.keyboard.down("Shift")
            await page.keyboard.press("Enter")
            await page.keyboard.up("Shift")

        await page.keyboard.press("Enter")
        await page.wait_for_timeout(1000)
        return True
    except Exception as e:
        log_debug(f"❌ Erro enviando mensagem: {e}")
        try:
            await page.screenshot(path="debug_wa_send_err.png", full_page=True)
        except:
            pass
        return False

async def send_to_whatsapp_with_timeout(message: str, chat_name: str) -> bool:
    pw = ctx = page = None
    try:
        async with asyncio.timeout(WHATSAPP_TIMEOUT):
            result = await open_whatsapp_web()
            if not result:
                return False
            pw, ctx, page = result
            if not await find_and_select_chat(page, chat_name):
                return False
            ok = await send_message_whatsapp(page, message)
            if ok:
                log_debug("✅ Mensagem enviada com sucesso!")
            return ok
    except asyncio.TimeoutError:
        log_debug("⏰ Timeout no envio WhatsApp")
        return False
    finally:
        try:
            if ctx: await ctx.close()
        except: pass
        try:
            if pw: await pw.stop()
        except: pass

# =========================
# FORMATAÇÃO E JOBS
# =========================

def build_message(quotes: List[Dict[str, Any]]) -> str:
    hoje = datetime.now(TZ).strftime("%d/%m/%Y")
    linhas = [f"📊 Cotações do Dia — {hoje}"]
    if not quotes:
        linhas.append("• Sem dados disponíveis")
    else:
        for it in quotes:
            nome = it.get("nome", "Item")
            valor = it.get("valor")
            from_cache = it.get("from_cache", False)
            if valor is None:
                linhas.append(f"• {nome}: — (sem dados)")
            else:
                val_str = f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                cache_tag = " 📦" if from_cache else " 🆕"
                linhas.append(f"• {nome}: {val_str}{cache_tag}")
    linhas.append("")
    linhas.append("↪️ CEPEA/ESALQ • Bot automatizado")
    return "\n".join(linhas)

async def job_coletar():
    start = datetime.now()
    log_debug("=== INICIANDO COLETA ===")
    quotes = await get_quotes_with_timeout()
    msg = build_message(quotes)

    print("\n" + "="*50)
    print("RESULTADO DA COLETA:")
    print("="*50)
    for it in quotes:
        nome = it.get("nome", "?")
        valor = it.get("valor")
        from_cache = it.get("from_cache", False)
        metodo = it.get("metodo", "")
        status = "📦 CACHE" if from_cache else "🆕 NOVO"
        if valor is None:
            print(f"• {nome}: — [{status}]")
        else:
            val_str = f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            print(f"• {nome}: {val_str} [{status}] ({metodo})")
    print("="*50)

    try:
        Path("preview_coletas.txt").write_text(msg, encoding="utf-8")
        print("📄 Preview salvo: preview_coletas.txt")
    except Exception as e:
        log_debug(f"Erro salvando preview: {e}")

    elapsed = (datetime.now() - start).total_seconds()
    log_debug(f"=== COLETA FINALIZADA em {elapsed:.1f}s ===")
    return msg

async def job_collect_and_send():
    log_debug("🚀 Iniciando coleta e envio...")
    message = await job_coletar()
    if not message:
        log_debug("❌ Falha na coleta - abortando envio")
        return False

    config = load_config()
    chat_name = config.get("whatsapp_chat_name", "Comunidad Agro – Anúncios")
    log_debug(f"📱 Enviando para: {chat_name}")
    ok = await send_to_whatsapp_with_timeout(message, chat_name)
    log_debug("✅ Envio concluído" if ok else "❌ Falha no envio WhatsApp")
    return ok

def load_config() -> Dict[str, Any]:
    default = {
        "whatsapp_chat_name": "Comunidad Agro – Anúncios",
        "send_time_local": "08:00",
        "timezone": "America/Belem",
    }
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            for k, v in default.items():
                cfg.setdefault(k, v)
            return cfg
        except Exception as e:
            log_debug(f"Erro carregando config: {e}")
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            yaml.safe_dump(default, f, allow_unicode=True)
        log_debug("✅ Config padrão criado")
    except Exception as e:
        log_debug(f"Erro criando config: {e}")
    return default

# =========================
# AGENDAMENTO
# =========================

def setup_scheduler():
    cfg = load_config()
    hhmm = cfg.get("send_time_local", "08:00")
    log_debug(f"📅 Agendando envio diário às {hhmm}")

    def run_job():
        if shutdown_flag.is_set():
            return
        try:
            asyncio.run(job_collect_and_send())
        except Exception as e:
            log_debug(f"Erro no job agendado: {e}")

    schedule.every().day.at(hhmm).do(run_job)
    return hhmm

def run_scheduler():
    hhmm = setup_scheduler()
    print(f"🤖 Bot iniciado em modo agendado")
    print(f"📅 Envio diário às {hhmm}")
    print("🛑 Pressione Ctrl+C para parar\n")
    while not shutdown_flag.is_set():
        try:
            schedule.run_pending()
            time.sleep(5)
        except KeyboardInterrupt:
            break
        except Exception as e:
            log_debug(f"Erro no loop do agendador: {e}")
            time.sleep(10)
    log_debug("🛑 Agendador parado")

# =========================
# MAIN / CLI
# =========================

def show_help():
    print("""
🤖 Bot de Cotações WhatsApp - Versão Indicador (CEPEA)

COMANDOS:
  --collect      Apenas coleta e exibe cotações (não envia)
  --send-now     Coleta e envia imediatamente para WhatsApp
  --schedule     Inicia modo agendado (envia diariamente)
  --test         Teste rápido (Soja)
  --config       Mostra configurações atuais

Arquivos:
  - config.yaml
  - cache_cotacoes.json
  - preview_coletas.txt
  - debug_log.txt
  - wa_profile/
""")

async def main_with_timeout():
    try:
        async with asyncio.timeout(360):
            await job_coletar()
    except asyncio.TimeoutError:
        print("\n⏰ TIMEOUT TOTAL DO PROGRAMA - encerrando...")
    finally:
        shutdown_flag.set()
        print("\n🏁 Programa finalizado!")

async def main_send_now():
    try:
        async with asyncio.timeout(480):
            ok = await job_collect_and_send()
            print("\n✅ Mensagem enviada com sucesso!" if ok else "\n❌ Falha no envio da mensagem")
    except asyncio.TimeoutError:
        print("\n⏰ TIMEOUT no envio - processo encerrado")
    finally:
        shutdown_flag.set()
        print("\n🏁 Programa finalizado!")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Bot de cotações WhatsApp - indicador CEPEA (BRL only)",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--collect", action="store_true", help="Coleta e exibe cotações (sem enviar)")
    parser.add_argument("--send-now", action="store_true", help="Coleta e envia imediatamente para WhatsApp")
    parser.add_argument("--schedule", action="store_true", help="Envio diário (usa config.yaml)")
    parser.add_argument("--test", action="store_true", help="Teste rápido (Soja)")
    parser.add_argument("--config", action="store_true", help="Mostrar configurações atuais")
    args = parser.parse_args()

    if not any(vars(args).values()):
        show_help()
        sys.exit(0)

    if args.test:
        async def _t():
            log_debug("=== TESTE RÁPIDO ===")
            r = await collect_commodity_fast("Soja (sc)", "soja")
            if r:
                print(f"✅ Soja: R$ {r['valor']:.2f} ({r['metodo']})")
            else:
                print("❌ Teste falhou")
            log_debug("=== FIM TESTE ===")
        try:
            asyncio.run(_t())
        except KeyboardInterrupt:
            print("\n🛑 Teste interrompido")
        sys.exit(0)

    if args.config:
        print("\n📋 CONFIGURAÇÕES ATUAIS:")
        print("="*40)
        cfg = load_config()
        for k, v in cfg.items():
            print(f"{k}: {v}")
        print("="*40)
        print(f"Arquivo: {CONFIG_PATH.absolute()}")
        sys.exit(0)

    if args.collect:
        print("🚀 Iniciando coleta (sem envio)...")
        try:
            asyncio.run(main_with_timeout())
        except KeyboardInterrupt:
            print("\n🛑 Coleta interrompida")
        sys.exit(0)

    if args.send_now:
        print("📱 Prepare-se para escanear QR code se necessário")
        try:
            asyncio.run(main_send_now())
        except KeyboardInterrupt:
            print("\n🛑 Envio interrompido")
        sys.exit(0)

    if args.schedule:
        try:
            run_scheduler()
        except KeyboardInterrupt:
            print("\n🛑 Agendamento interrompido pelo usuário")
        finally:
            shutdown_flag.set()
        sys.exit(0)
