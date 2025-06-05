"""
Utilitários para verificação e monitoramento de conectividade de rede.
"""

import asyncio
import logging
import socket
import time
from typing import Tuple, List, Dict
import aiohttp
import requests

logger = logging.getLogger(__name__)


def check_internet_connection() -> Tuple[bool, str]:
    """
    Verifica se há conexão com a internet usando múltiplos métodos.
    
    Returns:
        Tuple[bool, str]: (conectado, mensagem_status)
    """
    try:
        # Método 1: Tentar HTTP request para Google
        response = requests.get("http://www.google.com", timeout=10)
        if response.status_code == 200:
            return True, "Conectividade HTTP confirmada (Google)"
    except requests.RequestException:
        pass
    
    try:
        # Método 2: Tentar conexão TCP para DNS público
        socket.create_connection(("8.8.8.8", 53), timeout=10)
        return True, "Conectividade TCP confirmada (DNS Google)"
    except OSError:
        pass
    
    try:
        # Método 3: Tentar conexão TCP para Cloudflare DNS
        socket.create_connection(("1.1.1.1", 53), timeout=10)
        return True, "Conectividade TCP confirmada (DNS Cloudflare)"
    except OSError:
        pass
    
    return False, "Sem conectividade de rede detectada"


async def check_connection_quality(test_urls: List[str] = None) -> Dict[str, any]:
    """
    Verifica a qualidade da conexão de rede testando latência e velocidade.
    
    Args:
        test_urls: URLs para testar (opcional)
        
    Returns:
        Dict com métricas de qualidade da conexão
    """
    if test_urls is None:
        test_urls = [
            "http://www.google.com",
            "http://www.github.com",
            "http://httpbin.org/get"
        ]
    
    results = {
        "total_tests": len(test_urls),
        "successful_tests": 0,
        "failed_tests": 0,
        "average_latency_ms": 0,
        "min_latency_ms": float('inf'),
        "max_latency_ms": 0,
        "connection_quality": "unknown",
        "errors": []
    }
    
    latencies = []
    
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30, connect=10)
    ) as session:
        
        for url in test_urls:
            try:
                start_time = time.time()
                
                async with session.get(url) as response:
                    await response.read()  # Ler resposta completa
                    
                end_time = time.time()
                latency_ms = (end_time - start_time) * 1000
                
                if response.status == 200:
                    results["successful_tests"] += 1
                    latencies.append(latency_ms)
                    results["min_latency_ms"] = min(results["min_latency_ms"], latency_ms)
                    results["max_latency_ms"] = max(results["max_latency_ms"], latency_ms)
                    logger.debug(f"Teste de conectividade para {url}: {latency_ms:.1f}ms")
                else:
                    results["failed_tests"] += 1
                    results["errors"].append(f"{url}: HTTP {response.status}")
                    
            except asyncio.TimeoutError:
                results["failed_tests"] += 1
                results["errors"].append(f"{url}: Timeout")
                logger.warning(f"Timeout ao testar {url}")
                
            except Exception as e:
                results["failed_tests"] += 1
                results["errors"].append(f"{url}: {str(e)}")
                logger.warning(f"Erro ao testar {url}: {e}")
    
    # Calcular métricas
    if latencies:
        results["average_latency_ms"] = sum(latencies) / len(latencies)
        
        # Determinar qualidade da conexão
        avg_latency = results["average_latency_ms"]
        success_rate = results["successful_tests"] / results["total_tests"]
        
        if success_rate >= 0.8 and avg_latency < 200:
            results["connection_quality"] = "excellent"
        elif success_rate >= 0.6 and avg_latency < 500:
            results["connection_quality"] = "good"
        elif success_rate >= 0.4 and avg_latency < 1000:
            results["connection_quality"] = "fair"
        else:
            results["connection_quality"] = "poor"
    else:
        results["min_latency_ms"] = 0
        results["connection_quality"] = "no_connection"
    
    logger.info(f"Qualidade da conexão: {results['connection_quality']} "
                f"(latência média: {results['average_latency_ms']:.1f}ms, "
                f"taxa de sucesso: {results['successful_tests']}/{results['total_tests']})")
    
    return results


async def test_download_speed(test_url: str = "http://httpbin.org/bytes/1048576") -> Dict[str, float]:
    """
    Testa a velocidade de download da conexão.
    
    Args:
        test_url: URL para teste de velocidade (padrão: 1MB do httpbin)
        
    Returns:
        Dict com métricas de velocidade
    """
    results = {
        "download_speed_mbps": 0,
        "download_time_seconds": 0,
        "bytes_downloaded": 0,
        "success": False
    }
    
    try:
        start_time = time.time()
        
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60)
        ) as session:
            async with session.get(test_url) as response:
                if response.status == 200:
                    data = await response.read()
                    end_time = time.time()
                    
                    download_time = end_time - start_time
                    bytes_downloaded = len(data)
                    speed_mbps = (bytes_downloaded * 8) / (download_time * 1024 * 1024)  # Mbps
                    
                    results.update({
                        "download_speed_mbps": speed_mbps,
                        "download_time_seconds": download_time,
                        "bytes_downloaded": bytes_downloaded,
                        "success": True
                    })
                    
                    logger.info(f"Velocidade de download: {speed_mbps:.2f} Mbps "
                               f"({bytes_downloaded / (1024*1024):.1f}MB em {download_time:.1f}s)")
                else:
                    logger.warning(f"Teste de velocidade falhou: HTTP {response.status}")
                    
    except Exception as e:
        logger.error(f"Erro no teste de velocidade: {e}")
    
    return results


def get_network_recommendations(connection_quality: str, download_speed_mbps: float) -> Dict[str, any]:
    """
    Fornece recomendações de configuração baseadas na qualidade da rede.
    
    Args:
        connection_quality: Qualidade da conexão ('excellent', 'good', 'fair', 'poor')
        download_speed_mbps: Velocidade de download em Mbps
        
    Returns:
        Dict com recomendações de configuração
    """
    recommendations = {
        "max_concurrent_downloads": 4,
        "timeout_multiplier": 1.0,
        "retry_attempts": 3,
        "chunk_size_kb": 64,
        "use_keep_alive": True,
        "connection_pool_size": 50
    }
    
    if connection_quality == "excellent" and download_speed_mbps > 50:
        recommendations.update({
            "max_concurrent_downloads": 8,
            "timeout_multiplier": 1.0,
            "retry_attempts": 2,
            "chunk_size_kb": 128,
            "connection_pool_size": 100
        })
    elif connection_quality == "good" and download_speed_mbps > 20:
        recommendations.update({
            "max_concurrent_downloads": 6,
            "timeout_multiplier": 1.2,
            "retry_attempts": 3,
            "chunk_size_kb": 64,
            "connection_pool_size": 75
        })
    elif connection_quality == "fair" or download_speed_mbps < 10:
        recommendations.update({
            "max_concurrent_downloads": 3,
            "timeout_multiplier": 1.5,
            "retry_attempts": 4,
            "chunk_size_kb": 32,
            "connection_pool_size": 30
        })
    elif connection_quality == "poor" or download_speed_mbps < 5:
        recommendations.update({
            "max_concurrent_downloads": 1,
            "timeout_multiplier": 2.0,
            "retry_attempts": 5,
            "chunk_size_kb": 16,
            "connection_pool_size": 10,
            "use_keep_alive": False
        })
    
    logger.info(f"Recomendações de rede: {recommendations['max_concurrent_downloads']} downloads simultâneos, "
                f"timeout x{recommendations['timeout_multiplier']}, "
                f"{recommendations['retry_attempts']} tentativas")
    
    return recommendations


async def adaptive_network_test() -> Dict[str, any]:
    """
    Executa um teste completo e adaptativo da rede.
    
    Returns:
        Dict com resultados completos e recomendações
    """
    logger.info("🌐 Iniciando teste adaptativo de rede...")
    
    # Verificação básica de conectividade
    is_connected, connection_msg = check_internet_connection()
    if not is_connected:
        return {
            "connected": False,
            "message": connection_msg,
            "recommendations": get_network_recommendations("no_connection", 0)
        }
    
    # Teste de qualidade da conexão
    quality_results = await check_connection_quality()
    
    # Teste de velocidade (apenas se a qualidade for razoável)
    speed_results = {"download_speed_mbps": 0, "success": False}
    if quality_results["connection_quality"] not in ["poor", "no_connection"]:
        speed_results = await test_download_speed()
    
    # Gerar recomendações
    recommendations = get_network_recommendations(
        quality_results["connection_quality"],
        speed_results["download_speed_mbps"]
    )
    
    results = {
        "connected": True,
        "message": connection_msg,
        "quality": quality_results,
        "speed": speed_results,
        "recommendations": recommendations
    }
    
    logger.info(f"✅ Teste de rede concluído: {quality_results['connection_quality']} "
                f"({speed_results['download_speed_mbps']:.1f} Mbps)")
    
    return results
