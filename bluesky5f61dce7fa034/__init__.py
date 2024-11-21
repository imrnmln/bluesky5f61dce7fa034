import aiohttp
from aiohttp.client_exceptions import ClientError, ServerDisconnectedError, ClientHttpProxyError
from aiohttp_socks import ProxyConnector, SocksConnector
from stem import Signal
from stem.control import Controller
import asyncio
import random
import logging
from datetime import datetime, timedelta
import hashlib
import requests
import re
from typing import AsyncGenerator, Any, Dict
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    ExternalId,
    Url,
    Domain,
)

ONLINE_KW_LIST_URL = "https://raw.githubusercontent.com/exorde-labs/TestnetProtocol/refs/heads/main/targets/keywords.txt"
logging.basicConfig(level=logging.INFO)


USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Edge/129.0.2792.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 OPR/114.0.0.0',
    'Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1'
]
# Constants
SPECIAL_KEYWORDS_LIST = [    
    "the",
    "the",
    "lord",
    "ords",
    "brc20",
    "paris2024",
    "paris2024",
    "olympic",
    "olympic",
    "Acura",
    "Alfa Romeo",
    "Aston Martin",
    "Audi",
    "Bentley",
    "BMW",
    "Buick",
    "Cadillac",
    "Chevrolet",
    "Chrysler",
    "Dodge",
    "Ferrari",
    "Fiat",
    "Ford",
    "Genesis",
    "GMC",
    "Honda",
    "Hyundai",
    "Infiniti",
    "Jaguar",
    "Jeep",
    "Kia",
    "Lamborghini",
    "Land Rover",
    "Lexus",
    "Lincoln",
    "Lotus",
    "Maserati",
    "Mazda",
    "Taiko",
    "Taiko labs",
    "McLaren",
    "Mercedes-Benz",
    "MINI",
    "Mitsubishi",
    "Nissan",
    "Porsche",
    "Ram",
    "Renault",
    "Rolls-Royce",
    "Subaru",
    "Tesla",
    "Toyota",
    "Volkswagen",
    "Volvo",    
    "BlackRock",
    "Vanguard",
    "State Street",
    "advisors",
    "Fidelity",
    "Fidelity Investments",
    "Asset Management",
    "Asset",
    "digital asset",
    "NASDAQ Composite",
    "Dow Jones Industrial Average",
    "Gold",
    "Silver",
    "Brent Crude",
    "WTI Crude",
    "EUR",
    "US",
    "YEN"
    "UBS",
    "PIMCO",
    "schroders",
    "aberdeen",    
    "louis vuitton",
    "moet Chandon",
    "hennessy",
    "dior",
    "fendi",
    "givenchy",
    "celine",
    "tag heuer",
    "bvlgari",
    "dom perignon",
    "hublot",
    "Zenith",    
    "meme", 
    "coin", 
    "memecoin", 
    "pepe", 
    "doge", 
    "shib",
    "floki",
    "dogtoken",
    "trump token",
    "barron token",    
    "DOGE",
    "SHIB",
    "PEPE",
    "BONK",
    "WIF",
    "FLOKI",
    "MEME",    
    "DOGE",
    "SHIB",
    "PEPE",
    "BONK",
    "WIF",
    "FLOKI",
    "MEME",
    "DOGE",
    "SHIB",
    "PEPE",
    "BONK",
    "WIF",
    "FLOKI",
    "MEME",
    "TRUMP",
    "BabyDoge",
    "ERC20",
    "BONE",
    "COQ",
    "WEN",
    "BITCOIN",
    "ELON",
    "SNEK",
    "MYRO",
    "PORK",
    "TOSHI",
    "SMOG",
    "LADYS",
    "AIDOGE",
    "TURBO",
    "TOKEN",
    "SAMO",
    "KISHU",
    "TSUKA",
    "LEASH",
    "QUACK",
    "VOLT",
    "PEPE2.0",
    "JESUS",
    "MONA",
    "DC",
    "WSM",
    "PIT",
    "QOM",
    "PONKE",
    "SMURFCAT",
    "AKITA",
    "VINU",
    "ANALOS",
    "BAD",
    "CUMMIES",
    "HONK",
    "HOGE",
    "$MONG",
    "SHI",
    "BAN",
    "RAIN",
    "TAMA",
    "PAW",
    "SPX",
    "HOSKY",
    "BOZO",
    "DOBO",
    "PIKA",
    "CCC",
    "REKT",
    "WOOF",
    "MINU",
    "WOW",
    "PUSSY",
    "KEKE",
    "DOGGY",
    "KINGSHIB",
    "CHEEMS",
    "SMI",
    "OGGY",
    "DINGO",
    "DONS",
    "GRLC",
    "AIBB",
    "CATMAN",
    "XRP",
    "CAT",
    "数字資産",  # Digital Asset (Japanese)
    "仮想",  # Virtual (Japanese)
    "仮想通貨",  # Virtual Currency (Japanese)
    "自動化",  # Automation (Japanese)
    "アルゴリズム",  # Algorithm (Japanese)
    "コード",  # Code (Japanese)
    "機械学習",  # Machine Learning (Japanese)
    "ブロックチェーン",  # Blockchain (Japanese)
    "サイバーセキュリティ",  # Cybersecurity (Japanese)
    "人工",  # Artificial (Japanese)
    "合成",  # Synthetic (Japanese)
    "主要",  # Major (Japanese)
    "IoT",
    "クラウド",  # Cloud (Japanese)
    "ソフトウェア",  # Software (Japanese)
    "API",
    "暗号化",  # Encryption (Japanese)
    "量子",  # Quantum (Japanese)
    "ニューラルネットワーク",  # Neural Network (Japanese)
    "オープンソース",  # Open Source (Japanese)
    "ロボティクス",  # Robotics (Japanese)
    "デブオプス",  # DevOps (Japanese)
    "5G",
    "仮想現実",  # Virtual Reality (Japanese)
    "拡張現実",  # Augmented Reality (Japanese)
    "バイオインフォマティクス",  # Bioinformatics (Japanese)
    "ビッグデータ",  # Big Data (Japanese)
    "大統領",  # President (Japanese)
    "行政",  # Administration (Japanese)
    "Binance",
    "Bitcoin ETF",
    "政治",  # Politics (Japanese)
    "政治的",  # Political (Japanese)
    "ダイアグラム",  # Diagram (Japanese)
    "$algo",
    "$algo",
    "%23CAC",
    "%23G20",
    "%23IPO",
    "%23NASDAQ",
    "%23NYSE",
    "%23OilPrice",
    "%23SP500",
    "%23USD",
    "%23airdrop",
    "%23altcoin",
    "%23bonds",
    "%23price",
    "AI",
    "AI",
    "AI",
    "AI",
    "AUDNZD",
    "Alphabet%20(GOOG)",
    "Apple",
    "Aprendizaje Automático",
    "BNB",
    "Berkshire",
    "Biden administration",
    "Binance",
    "Bitcoin%20ETF",
    "Black%20Rock",
    "BlackRock",
    "BlackRock",
    "Branche",
    "Brazil",
    "CAC40",
    "COIN",
    "Canada",
    "China",
    "Coinbase",
    "Congress",
    "Crypto",
    "Crypto",
    "Crypto",
    "Cryptocurrencies",
    "Cryptos",
    "DeFi",
    "Diagramm",
    "Dios mío",
    "DowJones",
    "ETF",
    "ETFs",
    "EU",
    "EU",
    "EURUSD",
    "Elon",
    "Elon",
    "Elon",
    "Elon%20musk",
    "Europe",
    "European%20union%20(EU)",
    "FB%20stock",
    "FTSE",
    "Firma",
    "France",
    "GDP",
    "GPU",
    "GameFi",
    "Gensler",
    "Germany",
    "Gerücht",
    "Geschäft",
    "Gesundheit",
    "Gewinn",
    "Gewinn",
    "Heilung",
    "IA",
    "IA",
    "IPO",
    "Israel",
    "Israel",
    "Israel",
    "Juego",
    "KI",
    "Konflikt",
    "Kraken",
    "LGBTQ rights",
    "LVMH",
    "Land",
    "Luxus",
    "Marke",
    "Maschinelles Lernen",
    "Mexico",
    "NFLX",
    "NFT",
    "NFT",
    "NFTs",
    "NYSE",
    "Nachrichten",
    "Nasdaq%20100",
    "Oh Dios mío",
    "Openfabric",
    "Openfabric AI",
    "Openfabric",
    "OFN",
    "PLTR",
    "Palestine",
    "Palestine",
    "Palestine",
    "País",
    "Politik",
    "Produkt",
    "Roe v. Wade",
    "Silicon Valley",
    "Spiel",
    "Spot%20ETF",
    "Start-up",
    "Streaming",
    "Supreme Court",
    "Technologie",
    "Tesla",
    "UE",
    "UE",
    "USA",
    "USDEUR",
    "United%20states",
    "Unterhaltung",
    "Verlust",
    "Virus",
    "Vorhersage",
    "WallStreet",
    "WarrenBuffett",
    "Warren Buffett",
    "Web3",
    "X.com",
    "XAUUSD",
    "Xitter",
    "abortion",
    "achetez",
    "actualité",
    "airdrop",
    "airdrops",
    "alert",
    "algorand",
    "algorand",
    "algorand",
    "amazon",
    "analytics",
    "announcement",
    "apprentissage",
    "artificial intelligence",
    "artificial intelligence",
    "asset",
    "asset%20management",
    "attack",
    "attack",
    "attack",
    "attentat",
    "authocraty",
    "balance sheet",
    "bank",
    "bear",
    "bearish",
    "bears",
    "beliebt",
    "bezos",
    "biden",
    "biden",
    "biden",
    "biden",
    "data",
    "develop",
    "virtual",
    "automation",
    "algorithm",
    "code",
    "machine learning",
    "blockchain",
    "cybersecurity",
    "artificial",
    "synth",
    "synthetic",
    "major",
    "IoT",
    "cloud",
    "software",
    "API",
    "encryption",
    "quantum",
    "neural",
    "open source",
    "robotics",
    "devop",
    "5G",
    "virtual reality",
    "augmented reality",
    "bioinformatics",
    "big data",
    "billion",
    "bitcoin",
    "bizness",
    "blockchain",
    "bond",
    "breaking news",
    "breaking%20news",
    "btc",
    "btc",
    "btc",
    "btc",
    "btc",
    "btc",
    "btc",
    "btc",
    "budget",
    "bull",
    "bullish",
    "bulls",
    "business",
    "businesses",
    "buy support",
    "cardano",
    "cash flow",
    "cbdc",
    "choquant",
    "climate change action",
    "climate change",
    "climate tech startups",
    "communist",
    "companies",
    "company",
    "compound interest",
    "compra ahora"
    "compra",
    "conflict",
    "conflict",
    "conflicto",
    "conflit",
    "congress",
    "conservatives",
    "corporate",
    "corporation",
    "credit",
    "crime",
    "crisis",
    "crude%20oil",
    "crypto",
    "crypto",
    "crypto",
    "crypto",
    "crypto",
    "cryptocurrency",
    "cryptocurrency",
    "cura",
    "currencies",
    "currency",
    "currency",
    "database",
    "debit",
    "debt",
    "debt",
    "decentralized finance",
    "decentralized",
    "decline",
    "deep learning",
    "defi",
    "democracy",
    "diffusion",
    "digital",
    "divertissement",
    "dividend",
    "doge",
    "dogecoin",
    "démarrage",
    "e-commerce",
    "economy",
    "economy",
    "education startups",
    "education",
    "elections",
    "elisee",
    "embargo",
    "embassy",
    "empresa",
    "entreprise",
    "entretenimiento",
    "equity",
    "erc20",
    "eth",
    "eth",
    "eth",
    "eth",
    "eth",
    "ethereum",
    "exchange rate",
    "expense",
    "extremism",
    "fair%20launch",
    "fascist",
    "finance",
    "finance",
    "financial advisor",
    "financial planning",
    "financing",
    "fintech",
    "fintech",
    "fintech",
    "fiscal policy",
    "fixed income",
    "foreign aid",
    "foreign exchange",
    "foreign policy",
    "forex",
    "forex",
    "founder CEO",
    "founders",
    "fusion",
    "gagner",
    "gain",
    "ganancia",
    "ganar",
    "gas",
    "gaza",
    "gaza",
    "gaza",
    "government",
    "governments",
    "graphique",
    "gross domestic product",
    "growth",
    "gráfico",
    "gun control",
    "gun violence prevention",
    "hamas",
    "hamas",
    "hamas",
    "hamas",
    "healthcare startups",
    "healthcare",
    "helion",
    "hft trading",
    "holdings",
    "hostage",
    "hostage",
    "immigration reform",
    "immigration",
    "impactante",
    "impactante",
    "impeachment",
    "income",
    "increíble",
    "increíble",
    "incroyable",
    "industria",
    "industrie",
    "inflation",
    "inflation",
    "infrastructure",
    "insider trading",
    "insider",
    "insurance",
    "intraday",
    "investing",
    "investment",
    "investor",
    "investors",
    "jerusalem",
    "jeu",
    "kaufen",
    "kremlin",
    "legal",
    "legal%20tender",
    "liability",
    "libertarian",
    "liquidity",
    "loan",
    "long",
    "lujo",
    "luxe",
    "machine learning",
    "macron",
    "macron",
    "macron",
    "en marche",
    "parti",
    "marca",
    "margin",
    "mark%20zuckerberg",
    "market capitalization",
    "market maker",
    "market",
    "markets",
    "marque",
    "mein Gott",
    "middle east",
    "middle east",
    "middle east",
    "million",
    "mint",
    "missile",
    "missile",
    "missile",
    "mon Dieu",
    "monero",
    "money",
    "mortgage",
    "moscow",
    "mutual fund",
    "nasdaq",
    "national security",
    "national%20emergency",
    "national%20security",
    "natural%20gas",
    "negocios",
    "net income",
    "net worth",
    "new project",
    "new startup",
    "news",
    "newsfeed",
    "newsflash",
    "nft",
    "nft%20latform",
    "nftcommunity",
    "nfts",
    "noticias",
    "nuclear",
    "official",
    "oil",
    "parliament",
    "pays",
    "perder",
    "perdido",
    "perdre",
    "perdu",
    "plummet",
    "police",
    "politician",
    "politicians",
    "politique",
    "polkadot",
    "polygon",
    "política",
    "populaire",
    "popular",
    "populism",
    "portfolio",
    "predicción",
    "press",
    "price-to-earnings ratio",
    "producto",
    "produit",
    "profit",
    "promising company",
    "protocols",
    "prédiction",
    "putin",
    "putin",
    "putin",
    "putin",
    "poutine",
    "poutine",
    "poutine",
    "poutine",
    "vladimir putin",
    "vladimir putin",
    "vladimir putin",
    "xi jinping",
    "xi jinping",
    "xi jinping",
    "racial justice",
    "recession",
    "renault trucks",
    "renault trucks",
    "renault",
    "renault",
    "resistance sell",
    "retirement planning",
    "return on investment",
    "riots",
    "ripple",
    "risk",
    "robotics",
    "rumeur",
    "rumor",
    "russia",
    "s&p500",
    "sales",
    "salud",
    "sam20altman",
    "santé",
    "satoshi",
    "schockierend",
    "scraping",
    "securities",
    "security%20token",
    "self-driving cars",
    "senate",
    "senator",
    "senators",
    "shardeum",
    "short",
    "silvio micali",
    "solana",
    "solana%20sol",
    "sp500",
    "space exploration",
    "space tech startups",
    "stablecoin",
    "startup",
    "startup",
    "stock market",
    "stock",
    "stocks",
    "streaming",
    "syria",
    "takeoff",
    "tax",
    "tech startups",
    "technologie",
    "technology",
    "tecnología",
    "token",
    "toyota",
    "trade",
    "trading",
    "trading",
    "traitement",
    "treasury bill",
    "trump",
    "trump",
    "trump",
    "trump",
    "vote",
    "vote",
    "vote",
    "election",
    "election",
    "election",
    "voter",
    "voter",
    "million",
    "club",
    "tech",
    "nvda",
    "machine",
    "generative",
    "reinforcement",
    "official",
    "twitter",
    "ukraine",
    "unglaublich",
    "unicorns",
    "unicorns",
    "us%20president",
    "usdt",
    "usdt",
    "usdt",
    "usdt",
    "utility%20token",
    "venture capital",
    "venture capital",
    "venture capital",
    "verloren",
    "virus",
    "virus",
    "volvo group",
    "volvo trucks",
    "volvo",
    "voting rights",
    "wall street",
    "war in Ukraine",
    "war",
    "web3",
    "web3",
    "white house",
    "worldcoin",
    "xrp",
    "yield",
    "zero knowledge",
    "zksync",    
    "renewables",
    "energy",
    "infrastructure",
    "infrastructure investment",
    "FDI investment",
    "foreign investment",
    "foreign policy",
    "new policy",
    "new policies",
    "ГПУ",
    "ЕС",
    "ИИ",
    "Игра",
    "Илон",
    "Машинное обучение",
    "Страна",
    "бизнес",
    "бренд",
    "вирус",
    "график",
    "здоровье",
    "индустрия",
    "компания",
    "конфликт",
    "купи сейчас",
    "лечение",
    "невероятный",
    "новости",
    "о боже мой",
    "победа",
    "политика",
    "популярный",
    "поражение",
    "потеря",
    "потоковая передача",
    "прибыль",
    "прогноз",
    "продукт",
    "развлечение",
    "роскошь",
    "слух",
    "стартап",
    "технологии",
    "шокирующий",
    "أخبار",
    "أعمال",
    "إيلون",
    "اشتر الآن",
    "الاتحاد الأوروبي",
    "الذكاء الاصطناعي",
    "بث مباشر",
    "بلد",
    "ترفيه",
    "تعلم الآلة",
    "تكنولوجيا",
    "توقع",
    "خسارة",
    "رائع",
    "ربح",
    "رسم بياني",
    "سياسة",
    "شائعة",
    "شركة ناشئة",
    "شركة",
    "شهير",
    "صادم",
    "صحة",
    "صراع",
    "صناعة",
    "ضائع",
    "علاج",
    "علامة تجارية",
    "فخامة",
    "فوز",
    "فيروس",
    "لعبة",
    "منتج",
    "وحدة معالجة الرسومات",
    "يا إلهي",
    "ああ、神様",
    "イーロン",
    "ウイルス",
    "エンターテインメント",
    "ゲーム",
    "スタートアップ",
    "ストリーミング",
    "チャート",
    "テクノロジー",
    "ニュース",
    "ビジネス",
    "ブランド",
    "业务",
    "予測",
    "产品",
    "人工智能",
    "人気",
    "今買う",
    "令人难以置信",
    "令人震惊",
    "会社",
    "信じられない",
    "健康",
    "健康",
    "公司",
    "冲突",
    "初创企业",
    "利益",
    "勝利",
    "品牌",
    "哦，我的天啊",
    "噂",
    "国",
    "国家",
    "图表",
    "埃隆",
    "失われた",
    "失去",
    "娱乐",
    "技术",
    "收益",
    "政治",
    "政治",
    "敗北",
    "新闻",
    "机器学习",
    "機械学習",
    "欧盟",
    "治疗",
    "治療",
    "流媒体",
    "游戏",
    "热门",
    "産業",
    "病毒",
    "立刻购买",
    "紛争",
    "行业",
    "衝撃的",
    "製品",
    "谣言",
    "豪华",
    "赢",
    "输",
    "预测",
    "高級"
    ]

BASE_KEYWORDS = [
    'the', 'of', 'and', 'a', 'in', 'to', 'is', 'that', 'it', 'for', 'on', 'you', 'this', 'with', 'as', 'I', 'be', 'at', 'by', 'from', 'or', 'an', 'have', 'not', 'are', 'but', 'we', 'they', 'which', 'one', 'all', 'their', 'there', 'can', 'has', 'more', 'do', 'if', 'will', 'about', 'up', 'out', 'who', 'get', 'like', 'when', 'just', 'my', 'your', 'what',
    'el', 'de', 'y', 'a', 'en', 'que', 'es', 'la', 'lo', 'un', 'se', 'no', 'con', 'una', 'por', 'para', 'está', 'son', 'me', 'si', 'su', 'al', 'desde', 'como', 'todo', 'está',
    '的', '是', '了', '在', '有', '和', '我', '他', '这', '就', '不', '要', '会', '能', '也', '去', '说', '所以', '可以', '一个',
    'का', 'है', 'हों', 'पर', 'ने', 'से', 'कि', 'यह', 'तक', 'जो', 'और', 'एक', 'हिंदी', 'नहीं', 'आप', 'सब', 'तो', 'मुझे', 'इस', 'को',
    'في', 'من', 'إلى', 'على', 'مع', 'هو', 'هي', 'هذا', 'تلك', 'ون', 'كان', 'لك', 'عن', 'ما', 'ليس', 'كل', 'لكن', 'أي', 'ودي', 'أين',
    'র', 'এ', 'আমি', 'যা', 'তা', 'হয়', 'হবে', 'তুমি', 'কে', 'তার', 'এখন', 'এই', 'কিন্তু', 'মাঠ', 'কি', 'আপনি', 'বাহী', 'মনে', 'তাহলে', 'কেন', 'থাক',
    'o', 'a', 'e', 'de', 'do', 'da', 'que', 'não', 'em', 'para', 'como', 'com', 'um', 'uma', 'meu', 'sua', 'se', 'este', 'esse', 'isto',
    'в', 'и', 'не', 'на', 'что', 'как', 'что', 'он', 'она', 'это', 'но', 'с', 'из', 'по', 'к', 'то', 'да', 'был', 'который', 'кто',
    'の', 'に', 'は', 'を', 'です', 'ます', 'た', 'て', 'いる', 'い', 'この', 'それ', 'あ', '等', 'や', 'も', 'もし', 'いつ', 'よ', 'お',
    'der', 'die', 'das', 'und', 'in', 'zu', 'von', 'mit', 'ist', 'an', 'bei', 'ein', 'eine', 'nicht', 'als', 'auch', 'so', 'wie', 'was', 'oder',
    'le', 'la', 'à', 'de', 'et', 'un', 'une', 'dans', 'ce', 'que', 'il', 'elle', 'est', 's', 'des', 'pour', 'par', 'au', 'en', 'si',
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
]

TOR_PORTS = [9050, 9052, 9054, 9056, 9058, 9060, 9062, 9064, 9066, 9068, 9070, 9072, 9074, 9076, 9078, 9080, 9082, 9084, 9086, 9088]

async def get_tor_session(proxy_type: str, socks_port: str) -> aiohttp.ClientSession:
    """Return a new aiohttp session configured to use Tor with either socks5 or socks5h."""
    
    # Validate proxy_type
    if proxy_type not in ["socks5", "socks5h"]:
        raise ValueError("proxy_type must be either 'socks5' or 'socks5h'")

    if not socks_port:
        socks_port = random.choice(TOR_PORTS)
        
    tor_proxy = f"{proxy_type}://127.0.0.1:{socks_port}"
    logging.info(f"[Tor] Fetching with proxy {tor_proxy}")
    if proxy_type == "socks5":
        connector = SocksConnector.from_url(tor_proxy)
    else:
        connector = ProxyConnector.from_url(tor_proxy)
        
    session = aiohttp.ClientSession(connector=connector)
    return session

async def fetch_with_tor(url: str, proxy_type: str, socks_port: str) -> dict:
    """Fetch the URL through Tor, retrying in case of rate limiting or errors."""
    try:
        async with await get_tor_session(proxy_type, socks_port) as session:
            logging.info(f"[Tor] Fetching {url} with Tor")
            async with session.get(url, headers={"User-Agent": random.choice(USER_AGENT_LIST)}, timeout=30) as response:
                if response.status == 200:
                    content_type = response.headers.get('Content-Type', '').lower()
                    if 'application/json' in content_type:
                        return await response.json()
                    else:
                        logging.warning(f"[Tor] Unexpected content type: {content_type}")
                        return {}
                elif response.status == 429:
                    logging.warning(f"[Tor] Rate limit encountered for {url}, return nothing...")
                    return {}
                else:
                    logging.warning(f"[Tor] Error fetching {url} with status: {response.status}")
                    return {}
    except Exception as e:
        logging.warning(f"[Tor] Error: {e}")
        return {}

async def fetch_posts(session: aiohttp.ClientSession, keyword: str, since: str) -> list:
    url = f"https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts?q={keyword}&since={since}"
    logging.info(f"Fetching posts from {url}")
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.json()
            return data.get('posts', [])
        elif response.status == 429:
            socks_port = random.choice(TOR_PORTS)
            data = await fetch_with_tor(url, "socks5", socks_port)
            return data.get('posts', [])
        else:
            logging.error(f"Failed to fetch posts for keyword {keyword}: {response.status}")
            return []
        
def fetch_keywords_list() -> list:
    # Fetch the list of keywords from the online source, ONLINE_KW_LIST_URL
    try:
        # remote file is a list of comma-separated keywords
        response = requests.get(ONLINE_KW_LIST_URL, timeout=1)
        if response.status_code == 200:
            keywords_list = response.text.split(",")
            # remove any empty strings, and strip leading/trailing whitespace, and \n
            keywords_list = [kw.strip() for kw in keywords_list if kw.strip()]
            return keywords_list
    except Exception as e:
        logging.error(f"Failed to fetch keywords list: {e}")
        return None

def calculate_since(max_oldness_seconds: int) -> str:
    # Calculate the 'since' timestamp in ISO 8601 format
    since_time = datetime.utcnow() - timedelta(seconds=max_oldness_seconds)
    return since_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def convert_to_web_url(uri: str, user_handle: str) -> str:
    base_url = "https://bsky.app/profile"
    post_id = uri.split("/")[-1]
    web_url = f"{base_url}/{user_handle}/post/{post_id}"
    return web_url

def format_date_string1(date_string: str) -> str:
    """
    Parse and format a date string into a consistent ISO 8601 format.
    """
    # Match ISO8601/RFC3339 pattern
    match = re.match(
        r"^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\.[0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})?$", 
        date_string
    )
    if not match:
        raise ValueError(f"Unsupported date format: {date_string}")
    
    base, fractional, suffix = match.groups()
    
    # Truncate fractional seconds to 6 digits (microseconds) if present
    if fractional:
        fractional = fractional[:7]  # Includes the dot (e.g., ".933918")
    else:
        fractional = ""
    
    # Default suffix to UTC 'Z' if no timezone is present
    if not suffix:
        suffix = "Z"
    
    # Reassemble the standardized date
    standardized_date = f"{base}{fractional}{suffix}"
    
    # List of acceptable date formats
    date_formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",      # With fractional seconds and UTC
        "%Y-%m-%dT%H:%M:%S.%f%z",     # With fractional seconds and timezone offset
        "%Y-%m-%dT%H:%M:%SZ",         # Without fractional seconds, UTC
        "%Y-%m-%dT%H:%M:%S%z",        # Without fractional seconds, timezone offset
        "%Y-%m-%dT%H:%M:%S.%f",       # With fractional seconds, no timezone
        "%Y-%m-%dT%H:%M:%S",          # Without fractional seconds, no timezone
    ]
    
    # Attempt parsing using defined formats
    for fmt in date_formats:
        try:
            dt = datetime.strptime(standardized_date, fmt)
            # Return ISO 8601-compliant string
            return dt.isoformat()
        except ValueError:
            continue
    
    # If no format matches, raise an error
    raise ValueError(f"Unsupported date format after standardization: {standardized_date}")

def format_date_string(date_string: str) -> str:
    # Try parsing the date string with milliseconds and 'Z' suffix
    try:
        dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        # If the previous format doesn't match, try parsing without 'Z' suffix
        try:
            dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            # If the previous format doesn't match, try parsing with timezone offset
            try:
                dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f%z")
            except ValueError:
                # If the previous format doesn't match, try parsing without milliseconds and with timezone offset
                try:
                    dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S%z")
                except ValueError:
                    # If none of the formats match, raise an exception
                    raise ValueError(f"Unsupported date format: {date_string}")

    formatted_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return formatted_timestamp
DEFAULT_OLDNESS_SECONDS = 3600
DEFAULT_MAXIMUM_ITEMS = 20
DEFAULT_MIN_POST_LENGTH = 10

def read_parameters(parameters):
    # Check if parameters is not empty or None
    if parameters and isinstance(parameters, dict):
        try:
            max_oldness_seconds = parameters.get(
                "max_oldness_seconds", DEFAULT_OLDNESS_SECONDS
            )
        except KeyError:
            max_oldness_seconds = DEFAULT_OLDNESS_SECONDS

        try:
            maximum_items_to_collect = parameters.get(
                "maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS
            )
        except KeyError:
            maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS

        try:
            min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
        except KeyError:
            min_post_length = DEFAULT_MIN_POST_LENGTH

    else:
        # Assign default values if parameters is empty or None
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH

    return (
        max_oldness_seconds,
        maximum_items_to_collect,
        min_post_length
    )


async def query(parameters: dict) -> AsyncGenerator[Dict[str, Any], None]:
    max_oldness_seconds, maximum_items_to_collect, min_post_length = read_parameters(parameters)
    yielded_items = 0
    
    # try fetching from the online source
    try:
        logging.info(f"[Bluesky parameters] fetching keywords list from {ONLINE_KW_LIST_URL}")
        keywords_list = fetch_keywords_list()
    except Exception as e:
        logging.exception(f"[Bluesky parameters] Keywords list fetch failed: {e}")
        keywords_list = None

    
    for _ in range(4):
        if yielded_items >= maximum_items_to_collect:
            break
        
        if keywords_list is not None and keywords_list != []:
            search_keyword = random.choice(keywords_list)+";"+random.choice(keywords_list)+";"+random.choice(keywords_list)
            logging.info(f"[Bluesky parameters] using online keyword: {search_keyword}")
            # if it fails, use a base keyword
        else:
            search_keyword = random.choice(BASE_KEYWORDS)+";"+random.choice(BASE_KEYWORDS)+";"+random.choice(BASE_KEYWORDS)
            logging.info(f"[Bluesky parameters] using base keyword: {search_keyword}")
        # 15% of the time, use a special keyword
        if random.random() < 0.15:
            search_keyword = random.choice(SPECIAL_KEYWORDS_LIST)+";"+random.choice(SPECIAL_KEYWORDS_LIST)+";"+random.choice(SPECIAL_KEYWORDS_LIST)
            logging.info(f"[Bluesky parameters] using special keyword: {search_keyword}")

        since = calculate_since(max_oldness_seconds)
        logging.info(f"[Bluesky] Fetching posts for keyword '{search_keyword}' since {since}")

        async with aiohttp.ClientSession() as session:
            #posts = await fetch_posts(session, search_keyword, since)
            keywords = search_keyword.split(";")
            tasks = [fetch_posts(session, keyword, since) for keyword in keywords]
            results = await asyncio.gather(*tasks)
            for posts in results:
                for post in posts:
                    try:
                        if yielded_items >= maximum_items_to_collect:
                            break
                        
                        datestr =  format_date_string(post['record']["createdAt"])
                        # convert date to isoformat like 2021-09-01T00:00:00.000Z
                        author_handle = post["author"]["handle"]
                        # anonymize author_handle with a hash
                        sha1 = hashlib.sha1()
                        # Update the hash with the author string encoded to bytest
                        try:
                            author_ = author_handle
                        except:
                            author_ = "unknown"
                        sha1.update(author_.encode())
                        author_sha1_hex = sha1.hexdigest()
                        url_recomposed = convert_to_web_url(post["uri"],author_handle)
                        full_content = post["record"]["text"] + " " + " ".join(
                                image.get("alt", "") for image in post.get("record", {}).get("embed", {}).get("images", [])
                            ),
    
                        # log print the found post with url, date, content
                        logging.info(f"[Bluesky] Found post: url: %s, date: %s, content: %s", url_recomposed, datestr, full_content)
    
                        item_ = Item(
                            content=Content(str(full_content)),
                            author=Author(str(author_sha1_hex)),
                            created_at=CreatedAt(str(datestr)),
                            domain=Domain("bsky.app"),
                            external_id=ExternalId(post["uri"]),
                            url=Url(url_recomposed),
                        )
                        yielded_items += 1
                        yield item_
                    except Exception as e:
                        logging.exception(f"[Bluesky] Error processing post: {e}")
