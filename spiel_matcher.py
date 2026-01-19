"""
Agent Spiel Matching Module
Uses difflib.SequenceMatcher for fuzzy matching (70% threshold)
Matches outgoing messages against agent-specific opening/closing spiels
Supports MAIN and BINGO page categories
"""
from difflib import SequenceMatcher
import re

SPIEL_SIMILARITY_THRESHOLD = 0.70

# Agent-specific spiels with key phrases for SQL pre-filtering
# Format: "AGENT_NAME": {
#   "MAIN": {"opening": (full_spiel, [key_phrases]), "closing": (full_spiel, [key_phrases])},
#   "BINGO": {"opening": (full_spiel, [key_phrases]), "closing": (full_spiel, [key_phrases])}
# }
AGENT_SPIELS = {
    "MAI": {
        "MAIN": {
            "opening": (
                "What a JUANderful day! Paano po kita matutulungan Juankada?",
                ["juanderful day", "matutulungan juankada"]
            ),
            "closing": (
                "Thank you for messaging us Juankada! Please don't hesitate to reach out us again",
                ["thank you for messaging", "juankada", "reach out us again"]
            )
        },
        "BINGO": {
            "opening": (
                "What a JUANderful day! Paano po kita matutulungan?",
                ["juanderful day", "matutulungan"]
            ),
            "closing": (
                "Thank you for messaging us! Please don't hesitate to reach out us again",
                ["thank you for messaging", "reach out us again"]
            )
        }
    },
    "STEVE": {
        "MAIN": {
            "opening": (
                "Hello Juankada, I just JUANted to ask if you need any assistance. I'd be happy to help.",
                ["juankada", "juanted to ask", "assistance", "happy to help"]
            ),
            "closing": (
                "Good luck Juankada! Play smart, play responsibly, and message us anytime if you need help.",
                ["good luck juankada", "play smart", "play responsibly", "message us anytime"]
            )
        },
        "BINGO": {
            "opening": (
                "Hello, I just wanted to ask if you need any assistance. I'd be happy to help.",
                ["wanted to ask", "assistance", "happy to help"]
            ),
            "closing": (
                "Thank you for reaching out. We appreciate you, and we're here anytime you need help. Take care and enjoy the game.",
                ["thank you for reaching out", "appreciate you", "enjoy the game"]
            )
        }
    },
    "JAM": {
        "MAIN": {
            "opening": (
                "Good day juankada ano po maitutulong ko sa inyo today?",
                ["good day juankada", "maitutulong ko"]
            ),
            "closing": (
                "Maraming salamat, Juankada! Nandito lang kami always happy to assist po",
                ["maraming salamat", "juankada", "nandito lang kami", "happy to assist"]
            )
        },
        "BINGO": {
            "opening": (
                "Good day po ano po maitutulong ko sa inyo today?",
                ["good day po", "maitutulong ko"]
            ),
            "closing": (
                "Maraming salamat! Nandito lang kami always happy to assist po",
                ["maraming salamat", "nandito lang kami", "happy to assist"]
            )
        }
    },
    "KRISTIA": {
        "MAIN": {
            "opening": (
                "Kamusta JUANkada! Thanks for reaching out, game na game kaming tumulong! ano pong concern natin today?",
                ["kamusta juankada", "game na game", "tumulong", "concern natin"]
            ),
            "closing": (
                "Thanks for reaching out. If may tanong pa po, message ka lang po ulit, JUANkada. Happy to help always!",
                ["thanks for reaching out", "tanong pa po", "message ka lang", "juankada", "happy to help"]
            )
        },
        "BINGO": {
            "opening": (
                "Kamusta! Thanks for reaching out, game na game kaming tumulong! ano pong concern natin today?",
                ["kamusta", "game na game", "tumulong", "concern natin"]
            ),
            "closing": (
                "Thanks for reaching out po! If may tanong pa po, message ka lang po ulit. Happy to help always!",
                ["thanks for reaching out", "tanong pa po", "message ka lang", "happy to help"]
            )
        }
    },
    "DUSTINE": {
        "MAIN": {
            "opening": (
                "Hello po Juankada! Kamusta po kayo and How can we help you?",
                ["hello po juankada", "kamusta po kayo", "how can we help"]
            ),
            "closing": (
                "Maraming Salamat po Juankada! Sana po nakatulong po ako sayo sa araw na ito",
                ["maraming salamat", "juankada", "nakatulong", "araw na ito"]
            )
        },
        "BINGO": {
            "opening": (
                "Hello po! Kamusta po kayo and How can we help you?",
                ["hello po", "kamusta po kayo", "how can we help"]
            ),
            "closing": (
                "Maraming Salamat po! Sana po nakatulong po ako sayo sa araw na ito",
                ["maraming salamat", "nakatulong", "araw na ito"]
            )
        }
    },
    "KURT": {
        "MAIN": {
            "opening": (
                "Hello Juankada! Nandito lang kami if you need help po",
                ["hello juankada", "nandito lang kami", "need help"]
            ),
            "closing": (
                "Nandito lang kami para tumulong, wag kang mag-alala po. Good luck po, and always remember to stay in control and play responsibly",
                ["nandito lang kami", "tumulong", "good luck", "stay in control", "play responsibly"]
            )
        },
        "BINGO": {
            "opening": (
                "Hi po! How may we help you po?",
                ["hi po", "how may we help"]
            ),
            "closing": (
                "Goodluck po laging tatandaan na stay in control and always play responsibly po",
                ["goodluck", "stay in control", "play responsibly"]
            )
        }
    },
    "MIGUI": {
        "MAIN": {
            "opening": (
                "Good day Juankada! Ano po ang maitutulong namin sa'yo?",
                ["good day juankada", "maitutulong namin"]
            ),
            "closing": (
                "If may mga dagdag katanungan po kayo, feel free to message us anytime juankada. Thank you and good luck po!",
                ["dagdag katanungan", "feel free to message", "juankada", "good luck"]
            )
        },
        "BINGO": {
            "opening": (
                "Good day po! Ano po ang maitutulong namin sa'yo?",
                ["good day po", "maitutulong namin"]
            ),
            "closing": (
                "If may mga dagdag katanungan po kayo, feel free to message us anytime po. Thank you and good luck po!",
                ["dagdag katanungan", "feel free to message", "good luck"]
            )
        }
    },
    "AKI": {
        "MAIN": {
            "opening": (
                "Hello Juankada! How may we help you po?",
                ["hello juankada", "how may we help"]
            ),
            "closing": (
                "Thank you for reaching out. We truly appreciate you and always here to assist whenever you need support.",
                ["thank you for reaching out", "truly appreciate", "here to assist", "need support"]
            )
        },
        "BINGO": {
            "opening": (
                "Hello po How may we help you po?",
                ["hello po", "how may we help"]
            ),
            "closing": (
                "Thank you for reaching out. We truly appreciate you and always here to assist whenever you need support.",
                ["thank you for reaching out", "truly appreciate", "here to assist", "need support"]
            )
        }
    }
}

# Name mapping for database names that differ from AGENT_SPIELS keys
AGENT_NAME_MAP = {
    "migs": "MIGUI",
    "steven": "STEVE",
    "steve": "STEVE",
    "kristia": "KRISTIA",
    "mai": "MAI",
    "jam": "JAM",
    "dustine": "DUSTINE",
    "kurt": "KURT",
    "migui": "MIGUI",
    "aki": "AKI"
}

# Page to category mapping
PAGE_CATEGORY_MAP = {
    "Juan365 Cares": "MAIN",
    "Juan365 Careers": "MAIN",
    "Juan365 Live": "MAIN",
    "Juan365": "MAIN",
    "JuanBingo": "BINGO",
    "Juan Sports": "BINGO",
    "JuanSports": "BINGO"
}


def normalize_agent_name(name: str) -> str:
    """Normalize agent name to match AGENT_SPIELS keys."""
    if not name:
        return ""
    name_lower = name.lower().strip()
    return AGENT_NAME_MAP.get(name_lower, name.upper())


def get_page_category(page_name: str) -> str:
    """Get the category (MAIN or BINGO) for a page."""
    if not page_name:
        return "MAIN"
    return PAGE_CATEGORY_MAP.get(page_name, "MAIN")


def clean_text(text: str) -> str:
    """Clean text for comparison - remove emojis and extra whitespace."""
    if not text:
        return ""
    # Remove emojis and special characters
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        "]+", flags=re.UNICODE)
    text = emoji_pattern.sub('', text)
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text.lower()


def get_similarity(text1: str, text2: str) -> float:
    """Calculate similarity ratio between two texts."""
    if not text1 or not text2:
        return 0.0
    clean1 = clean_text(text1)
    clean2 = clean_text(text2)
    return SequenceMatcher(None, clean1, clean2).ratio()


def detect_spiel_owner(message: str, spiel_type: str, page_name: str = None) -> tuple:
    """
    Detect which agent's spiel was used in a message.
    Returns (agent_name, similarity_score) or (None, 0) if no match.

    Args:
        message: The message text to check
        spiel_type: "opening" or "closing"
        page_name: Optional page name to determine MAIN vs BINGO category
    """
    if not message:
        return None, 0

    category = get_page_category(page_name) if page_name else None
    best_match = None
    best_score = 0

    for agent_name, categories in AGENT_SPIELS.items():
        # Try both categories if page_name not specified
        categories_to_check = [category] if category else ["MAIN", "BINGO"]

        for cat in categories_to_check:
            if cat not in categories:
                continue

            spiel_config = categories[cat].get(spiel_type)
            if not spiel_config:
                continue

            spiel_text, key_phrases = spiel_config

            # Quick check: message must contain at least one key phrase
            message_lower = message.lower()
            has_key_phrase = any(phrase in message_lower for phrase in key_phrases)
            if not has_key_phrase:
                continue

            # Calculate similarity
            score = get_similarity(message, spiel_text)

            if score > best_score and score >= SPIEL_SIMILARITY_THRESHOLD:
                best_score = score
                best_match = agent_name

    return best_match, best_score


def count_spiels(agent_name: str, messages: list, page_name: str = None) -> tuple:
    """
    Count opening and closing spiels for a specific agent.
    Returns (opening_count, closing_count).
    """
    normalized_name = normalize_agent_name(agent_name)
    category = get_page_category(page_name) if page_name else "MAIN"

    config = AGENT_SPIELS.get(normalized_name, {}).get(category, {})
    if not config:
        return 0, 0

    opening_spiel = config.get("opening", ("", []))[0]
    closing_spiel = config.get("closing", ("", []))[0]

    opening_count = 0
    closing_count = 0

    for msg in messages:
        if opening_spiel and get_similarity(msg, opening_spiel) >= SPIEL_SIMILARITY_THRESHOLD:
            opening_count += 1
        if closing_spiel and get_similarity(msg, closing_spiel) >= SPIEL_SIMILARITY_THRESHOLD:
            closing_count += 1

    return opening_count, closing_count


def get_all_key_phrases(spiel_type: str = None) -> list:
    """Get all key phrases for SQL pre-filtering.

    Args:
        spiel_type: Optional - "opening" or "closing" to filter by type.
                   If None, returns all phrases.
    """
    phrases = set()
    for agent_config in AGENT_SPIELS.values():
        for category_config in agent_config.values():
            types_to_check = [spiel_type] if spiel_type else ["opening", "closing"]
            for st in types_to_check:
                if st in category_config:
                    phrases.update(category_config[st][1])
    return list(phrases)


def get_key_phrases(agent_name: str, spiel_type: str, page_name: str = None) -> list:
    """Get key phrases for a specific agent and spiel type for SQL pre-filtering."""
    normalized_name = normalize_agent_name(agent_name)
    category = get_page_category(page_name) if page_name else "MAIN"

    config = AGENT_SPIELS.get(normalized_name, {}).get(category, {})
    if not config:
        return []

    spiel_config = config.get(spiel_type)
    if not spiel_config:
        return []

    return spiel_config[1]  # Return key phrases list


def get_supported_agents() -> list:
    """Get list of agents with configured spiels."""
    return list(AGENT_SPIELS.keys())
