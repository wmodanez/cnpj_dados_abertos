def format_elapsed_time(seconds: float) -> str:
    """
    Formata o tempo decorrido em horas, minutos e segundos.
    
    Args:
        seconds: Tempo em segundos
        
    Returns:
        str: Tempo formatado (ex: "2h 15min 30s" ou "15min 30s" ou "30s")
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0 or hours > 0:  # Mostra minutos se tiver horas
        parts.append(f"{minutes}min")
    parts.append(f"{secs}s")
    
    return " ".join(parts) 