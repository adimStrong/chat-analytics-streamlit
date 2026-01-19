"""
Agent Spiel Matching Module
Uses difflib.SequenceMatcher for fuzzy matching (70% threshold)
Matches outgoing messages against agent-specific opening/closing spiels
"""
from difflib import SequenceMatcher
import re

SPIEL_SIMILARITY_THRESHOLD = 0.70

# Agent-specific spiels with key phrases for SQL pre-filtering
# Format: "AGENT_NAME": {"opening": (full_spiel, [key_phrases]), "closing": (full_spiel, [key_phrases])}
AGENT_SPIELS = {
    "MAI": {
        "opening": (
            "What a JUANderful day! Paano po kita matutulungan Juankada?",
            ["juanderful day", "matutulungan juankada"]
        ),
        "closing": (
            "Thank you for messaging us Juankada! Please don't hesitate to reach out",
            ["thank you for messaging", "juankada", "reach out"]
        )
    },
    "STEVE": {
        "opening": (
            "Hello Juankada, I just JUANted to ask if you need any assistance",
            ["juankada", "juanted to ask", "assistance"]
        ),
        "closing": (
            "Good luck Juankada! Play smart, play responsibly",
            ["good luck juankada", "play smart", "play responsibly"]
        )
    },
    "JAM": {
        "opening": (
            "Good day juankada ano po maitutulong ko sa inyo today?",
            ["good day juankada", "maitutulong ko"]
        ),
        "closing": (
            "Maraming salamat, Juankada! Nandito lang kami",
            ["maraming salamat", "juankada", "nandito lang"]
        )
    },
    "KRISTIA": {
        "opening": (
            "Kamusta JUANkada! Thanks for reaching out, game na game kaming tumulong",
            ["kamusta juankada", "game na game", "tumulong"]
        ),
        "closing": (
            "Thanks for reaching out. If may tanong pa po, message ka lang po ulit",
            ["thanks for reaching out", "tanong pa po", "message ka lang"]
        )
    },
    "DUSTINE": {
        "opening": (
            "Hello po Juankada! Kamusta po kayo and How can we help you?",
            ["hello po juankada", "kamusta po kayo", "how can we help"]
        ),
        "closing": (
            "Maraming Salamat po Juankada! Sana po nakatulong po ako",
            ["maraming salamat", "juankada", "nakatulong"]
        )
    },
    "KURT": {
        "opening": (
            "Hello Juankada! Nandito lang kami if you need help po",
            ["hello juankada", "nandito lang kami", "need help"]
        ),
        "closing": (
            "Good luck po, and always remember to stay in control",
            ["good luck po", "stay in control"]
        )
    },
    "MIGUI": {
        "opening": (
            "Good day Juankada! Ano po ang maitutulong namin sa'yo?",
            ["good day juankada", "maitutulong namin"]
        ),
        "closing": (
            "If may mga dagdag katanungan po kayo, feel free to message us anytime",
            ["dagdag katanungan", "feel free to message"]
        )
    },
    "AKI": {
        "opening": (
            "Hello Juankada! How may we help you po?",
            ["hello juankada", "how may we help"]
        ),
        "closing": (
            "Thank you for reaching out. We truly appreciate you",
            ["thank you for reaching out", "truly appreciate"]
        )
    }
}

# Name mapping for database names that differ from AGENT_SPIELS keys
# Maps database name (lowercase) to AGENT_SPIELS key (uppercase)
AGENT_NAME_MAP = {
    "migs": "MIGUI",
    "steven": "STEVE",
    "ally": "AKI",
    "tahari": "MAI",
}


def normalize_agent_name(name: str) -> str:
    """Normalize agent name to match AGENT_SPIELS keys."""
    if not name:
        return ""
    name_lower = name.lower()
    return AGENT_NAME_MAP.get(name_lower, name.upper())


def get_similarity(text1: str, text2: str) -> float:
    """Calculate similarity ratio between two strings using SequenceMatcher."""
    if not text1 or not text2:
        return 0.0
    return SequenceMatcher(None, text1.lower(), text2.lower()).ratio()


def clean_text(text: str) -> str:
    """Clean text for comparison - remove punctuation and extra whitespace."""
    if not text:
        return ""
    # Remove emojis and special characters, keep alphanumeric and spaces
    cleaned = re.sub(r'[^\w\s]', '', text.lower())
    # Normalize whitespace
    cleaned = ' '.join(cleaned.split())
    return cleaned


def match_spiel(message: str, spiel_text: str, threshold: float = SPIEL_SIMILARITY_THRESHOLD) -> bool:
    """
    Check if a message matches a spiel using fuzzy matching.

    Args:
        message: The outgoing message text to check
        spiel_text: The reference spiel text to match against
        threshold: Similarity threshold (default 0.70 = 70%)

    Returns:
        True if message matches spiel above threshold
    """
    if not message or not spiel_text:
        return False

    msg_clean = clean_text(message)
    spiel_clean = clean_text(spiel_text)

    # Check similarity
    similarity = get_similarity(msg_clean, spiel_clean)
    return similarity >= threshold


def count_spiels(agent_name: str, messages: list) -> tuple:
    """
    Count opening and closing spiels in a list of messages for a specific agent.

    Args:
        agent_name: The agent's name (case insensitive)
        messages: List of message texts (strings)

    Returns:
        Tuple of (opening_count, closing_count)
    """
    normalized_name = normalize_agent_name(agent_name)
    config = AGENT_SPIELS.get(normalized_name, {})
    if not config:
        return 0, 0

    opening_spiel = config.get("opening", ("", []))[0]
    closing_spiel = config.get("closing", ("", []))[0]

    opening_count = sum(1 for m in messages if m and match_spiel(m, opening_spiel))
    closing_count = sum(1 for m in messages if m and match_spiel(m, closing_spiel))

    return opening_count, closing_count


def get_key_phrases(agent_name: str, spiel_type: str) -> list:
    """
    Get SQL-friendly key phrases for pre-filtering messages.

    Args:
        agent_name: The agent's name
        spiel_type: Either "opening" or "closing"

    Returns:
        List of key phrases for SQL LIKE filtering
    """
    normalized_name = normalize_agent_name(agent_name)
    config = AGENT_SPIELS.get(normalized_name, {})
    return config.get(spiel_type, ("", []))[1]


def get_all_key_phrases(spiel_type: str) -> list:
    """
    Get all unique key phrases across all agents for a spiel type.
    Useful for broad pre-filtering.

    Args:
        spiel_type: Either "opening" or "closing"

    Returns:
        List of unique key phrases
    """
    phrases = set()
    for config in AGENT_SPIELS.values():
        if spiel_type in config:
            phrases.update(config[spiel_type][1])
    return list(phrases)


def get_agent_spiel_text(agent_name: str, spiel_type: str) -> str:
    """
    Get the full spiel text for an agent.

    Args:
        agent_name: The agent's name
        spiel_type: Either "opening" or "closing"

    Returns:
        The full spiel text or empty string if not found
    """
    config = AGENT_SPIELS.get(agent_name.upper(), {})
    return config.get(spiel_type, ("", []))[0]


def get_supported_agents() -> list:
    """Return list of agents that have spiels configured."""
    return list(AGENT_SPIELS.keys())


def detect_spiel_owner(message: str, spiel_type: str, threshold: float = SPIEL_SIMILARITY_THRESHOLD) -> tuple:
    """
    Detect which agent's spiel was used in a message.
    Returns the spiel OWNER, not necessarily the sending agent.

    Args:
        message: The outgoing message text to check
        spiel_type: Either "opening" or "closing"
        threshold: Similarity threshold (default 0.70 = 70%)

    Returns:
        Tuple of (owner_agent_name, similarity_score) or (None, 0.0) if no match
    """
    if not message:
        return None, 0.0

    msg_clean = clean_text(message)
    best_match = None
    best_score = 0.0

    for agent_name, config in AGENT_SPIELS.items():
        spiel_data = config.get(spiel_type)
        if not spiel_data:
            continue

        spiel_text = spiel_data[0]
        spiel_clean = clean_text(spiel_text)
        similarity = get_similarity(msg_clean, spiel_clean)

        if similarity > best_score:
            best_score = similarity
            best_match = agent_name

    if best_score >= threshold:
        return best_match, best_score
    return None, 0.0


def count_spiels_by_owner(messages: list) -> dict:
    """
    Count opening and closing spiels and attribute to spiel OWNER.
    If agent A uses agent B's spiel, it counts for agent B.

    Args:
        messages: List of message texts (strings)

    Returns:
        Dict of {agent_name: {"opening": count, "closing": count}}
    """
    results = {agent: {"opening": 0, "closing": 0} for agent in AGENT_SPIELS.keys()}

    for msg in messages:
        if not msg:
            continue

        # Check for opening spiel
        owner, score = detect_spiel_owner(msg, "opening")
        if owner:
            results[owner]["opening"] += 1

        # Check for closing spiel
        owner, score = detect_spiel_owner(msg, "closing")
        if owner:
            results[owner]["closing"] += 1

    return results


# For testing
if __name__ == "__main__":
    # Test matching
    print("Testing Spiel Matcher")
    print("=" * 50)

    # Test MAI opening
    test_msg = "What a JUANderful day! Paano po kita matutulungan Juankada?"
    mai_opening = AGENT_SPIELS["MAI"]["opening"][0]
    result = match_spiel(test_msg, mai_opening)
    print(f"MAI opening exact match: {result} (expected: True)")

    # Test with slight variation
    test_msg2 = "What a juanderful day paano kita matutulungan juankada"
    result2 = match_spiel(test_msg2, mai_opening)
    print(f"MAI opening fuzzy match: {result2} (expected: True)")

    # Test non-matching message
    test_msg3 = "Hello how are you today"
    result3 = match_spiel(test_msg3, mai_opening)
    print(f"Non-matching message: {result3} (expected: False)")

    # Test count_spiels
    messages = [
        "What a JUANderful day! Paano po kita matutulungan Juankada?",
        "Sure, let me check that for you",
        "Thank you for messaging us Juankada! Please don't hesitate to reach out",
        "Hello there",
        "What a JUANderful day! Paano po kita matutulungan Juankada?"
    ]
    opening, closing = count_spiels("MAI", messages)
    print(f"\nMAI spiels in 5 messages: Opening={opening}, Closing={closing} (expected: 2, 1)")

    print("\n" + "=" * 50)
    print("Supported agents:", get_supported_agents())
