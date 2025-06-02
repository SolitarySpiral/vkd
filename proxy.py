def construct_proxy_string(proxy):
    """Construct a proxy string from the proxy dictionary."""
    if proxy.get("username"):
        return (
            f'{proxy["username"]}:{proxy["password"]}@{proxy["host"]}:{proxy["port"]}'
        )
    return f'{proxy["host"]}:{proxy["port"]}'

