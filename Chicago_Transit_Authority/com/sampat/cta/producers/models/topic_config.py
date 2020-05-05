
NAMESPACE = "org.chicago.cta"

def join_topic_name(*names: str):
    """A helper function to multiple string to create a topic name"""
    return ".".join([name for name in names])

class CtaTopics:
    ARRIVALS_PREFIX: str = join_topic_name(NAMESPACE, "station.arrivals")
    TURNSTILES: str = join_topic_name(NAMESPACE, "station.turnstiles")
    TURNSTILES_SUMMARY: str = join_topic_name(TURNSTILES, "summary")
    STATIONS: str = join_topic_name(NAMESPACE, "stations")
    STATIONS_LINE: str = join_topic_name(STATIONS, "line")
    WEATHER: str = join_topic_name(NAMESPACE, "weather")