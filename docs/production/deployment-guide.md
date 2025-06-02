# 🏭 Guia de Deploy - Sistema de Processadores RF

## 📋 Visão Geral

Este guia orienta o deployment do sistema refatorado para ambientes de produção, cobrindo requisitos, configurações, monitoramento e estratégias de deploy seguro.

**Tipos de Deploy:**
- ✅ **Desenvolvimento**: Ambiente local e testes
- ✅ **Staging**: Ambiente de homologação
- ✅ **Produção**: Ambiente final otimizado
- ✅ **High-Availability**: Deploy com redundância

## 🎯 Requisitos de Sistema

### Ambiente de Desenvolvimento

```yaml
Hardware Mínimo:
  CPU: 4 núcleos (2.0GHz+)
  RAM: 8GB
  Disco: 100GB SSD
  
Software:
  OS: Windows 10+ / Ubuntu 20.04+
  Python: 3.9+
  Dependências: requirements.txt
  
Recursos:
  Workers: 2-4
  Memória por processo: 1-2GB
  Arquivos simultâneos: 5-10
```

### Ambiente de Staging

```yaml
Hardware Recomendado:
  CPU: 8 núcleos (2.5GHz+)
  RAM: 16GB
  Disco: 500GB SSD
  
Software:
  OS: Ubuntu 22.04 LTS / Windows Server 2022
  Python: 3.10+
  Monitoring: Logs estruturados
  
Recursos:
  Workers: 4-6
  Memória por processo: 2GB
  Arquivos simultâneos: 20-50
```

### Ambiente de Produção

```yaml
Hardware Otimizado:
  CPU: 16+ núcleos (3.0GHz+)
  RAM: 32-64GB
  Disco: 1TB+ NVME SSD
  Rede: 1Gbps+
  
Software:
  OS: Ubuntu 22.04 LTS (otimizado)
  Python: 3.11+ (compiled com PGO)
  Monitoring: Full stack observability
  Backup: Automatizado
  
Recursos:
  Workers: 8-16
  Memória por processo: 4GB
  Arquivos simultâneos: 100+
```

## 📦 Preparação para Deploy

### 1. Estrutura de Arquivos

```bash
# Estrutura recomendada para produção
projeto_rf/
├── app/
│   ├── src/                 # Código-fonte
│   ├── config/              # Configurações
│   ├── logs/                # Logs da aplicação
│   └── scripts/             # Scripts de deploy
├── data/
│   ├── input/               # Dados de entrada
│   ├── processing/          # Dados em processamento
│   └── output/              # Dados processados
├── backup/                  # Backups
├── monitoring/              # Métricas e monitoramento
└── deploy/
    ├── docker/              # Containers
    ├── systemd/             # Services Linux
    └── windows/             # Services Windows
```

### 2. Configuração de Dependências

```bash
# requirements_production.txt
polars>=0.20.0
pydantic>=2.0.0
psutil>=5.9.0
fastapi>=0.100.0  # Se usar API
uvicorn>=0.20.0   # Se usar API
prometheus-client>=0.15.0  # Métricas
structlog>=23.0.0  # Logs estruturados

# requirements_dev.txt (adicional para desenvolvimento)
pytest>=7.0.0
black>=23.0.0
mypy>=1.0.0
pre-commit>=3.0.0
```

### 3. Script de Build

```bash
#!/bin/bash
# build_production.sh

set -e

echo "🏗️  Iniciando build de produção..."

# 1. Limpar ambiente anterior
rm -rf dist/
rm -rf build/

# 2. Criar diretórios
mkdir -p dist/app/src
mkdir -p dist/config
mkdir -p dist/scripts
mkdir -p dist/logs

# 3. Copiar código-fonte
cp -r src/ dist/app/
cp -r config/ dist/
cp requirements_production.txt dist/

# 4. Compilar bytecode Python (otimização)
python -m compileall dist/app/src/

# 5. Criar configurações de produção
cat > dist/config/production.yaml << EOF
system:
  environment: production
  log_level: INFO
  max_workers: 16
  
processing:
  chunk_size: 50000
  memory_threshold: 0.90
  enable_cache: true
  
monitoring:
  metrics_enabled: true
  health_check_port: 8080
EOF

# 6. Criar script de inicialização
cat > dist/scripts/start_production.sh << 'EOF'
#!/bin/bash
cd /opt/rf_processors
source venv/bin/activate
export PYTHONPATH=/opt/rf_processors/app
python -m src.main --config=/opt/rf_processors/config/production.yaml
EOF

chmod +x dist/scripts/start_production.sh

echo "✅ Build concluído: dist/"
```

## 🐳 Deploy com Docker

### Dockerfile Otimizado

```dockerfile
# Dockerfile
FROM python:3.11-slim-bullseye

# Otimizações de sistema
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Configurar usuário não-root
RUN useradd -m -u 1000 rfuser
USER rfuser

# Diretório de trabalho
WORKDIR /app

# Copiar requirements primeiro (cache Docker)
COPY requirements_production.txt .
RUN pip install --no-cache-dir -r requirements_production.txt

# Copiar código-fonte
COPY --chown=rfuser:rfuser src/ ./src/
COPY --chown=rfuser:rfuser config/ ./config/

# Variáveis de ambiente
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Configurar recursos
ENV POLARS_MAX_THREADS=16
ENV OMP_NUM_THREADS=16

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import src.health_check; src.health_check.check()" || exit 1

# Comando padrão
CMD ["python", "-m", "src.main"]
```

### Docker Compose para Produção

```yaml
# docker-compose.prod.yml
version: '3.8'

services:
  rf-processor:
    build:
      context: .
      dockerfile: Dockerfile
    restart: unless-stopped
    
    environment:
      - ENV=production
      - LOG_LEVEL=INFO
      - MAX_WORKERS=16
    
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
      - ./config/production.yaml:/app/config/config.yaml:ro
    
    ports:
      - "8080:8080"  # Health check
    
    deploy:
      resources:
        limits:
          cpus: '16.0'
          memory: 32G
        reservations:
          cpus: '8.0'
          memory: 16G
    
    logging:
      driver: "json-file"
      options:
        max-size: "100m"
        max-file: "10"
  
  # Monitoramento
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
  
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=secure_password
    volumes:
      - grafana-data:/var/lib/grafana

volumes:
  grafana-data:
```

### Comandos de Deploy Docker

```bash
# Build da imagem
docker build -t rf-processors:latest .

# Deploy de produção
docker-compose -f docker-compose.prod.yml up -d

# Verificar status
docker-compose -f docker-compose.prod.yml ps

# Logs
docker-compose -f docker-compose.prod.yml logs -f rf-processor

# Scaling (se necessário)
docker-compose -f docker-compose.prod.yml up -d --scale rf-processor=3
```

## 🐧 Deploy Linux (SystemD)

### Service File

```ini
# /etc/systemd/system/rf-processors.service
[Unit]
Description=RF Data Processors
After=network.target
Wants=network.target

[Service]
Type=simple
User=rfuser
Group=rfuser
WorkingDirectory=/opt/rf_processors
Environment=PYTHONPATH=/opt/rf_processors
Environment=PYTHONUNBUFFERED=1
ExecStart=/opt/rf_processors/venv/bin/python -m src.main
ExecReload=/bin/kill -HUP $MAINPID
Restart=always
RestartSec=10

# Recursos
LimitNOFILE=65536
LimitNPROC=4096

# Logs
StandardOutput=journal
StandardError=journal
SyslogIdentifier=rf-processors

[Install]
WantedBy=multi-user.target
```

### Script de Instalação Linux

```bash
#!/bin/bash
# install_linux.sh

set -e

echo "🐧 Instalando RF Processors no Linux..."

# 1. Criar usuário de sistema
sudo useradd -r -s /bin/false rfuser

# 2. Criar diretórios
sudo mkdir -p /opt/rf_processors
sudo mkdir -p /var/log/rf_processors
sudo mkdir -p /var/lib/rf_processors

# 3. Copiar arquivos
sudo cp -r dist/* /opt/rf_processors/
sudo chown -R rfuser:rfuser /opt/rf_processors
sudo chown -R rfuser:rfuser /var/log/rf_processors
sudo chown -R rfuser:rfuser /var/lib/rf_processors

# 4. Criar ambiente virtual
sudo -u rfuser python3 -m venv /opt/rf_processors/venv
sudo -u rfuser /opt/rf_processors/venv/bin/pip install -r /opt/rf_processors/requirements_production.txt

# 5. Instalar service
sudo cp deploy/systemd/rf-processors.service /etc/systemd/system/
sudo systemctl daemon-reload

# 6. Habilitar e iniciar
sudo systemctl enable rf-processors
sudo systemctl start rf-processors

# 7. Verificar status
sudo systemctl status rf-processors

echo "✅ Instalação concluída!"
echo "📊 Status: sudo systemctl status rf-processors"
echo "📋 Logs: journalctl -u rf-processors -f"
```

## 🪟 Deploy Windows

### Windows Service

```python
# windows_service.py
import servicemanager
import socket
import sys
import win32event
import win32service
import win32serviceutil

class RFProcessorService(win32serviceutil.ServiceFramework):
    _svc_name_ = 'RFProcessors'
    _svc_display_name_ = 'RF Data Processors'
    _svc_description_ = 'Sistema de processamento de dados da Receita Federal'
    
    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        
    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        
    def SvcDoRun(self):
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        self.main()
        
    def main(self):
        import os
        import sys
        
        # Configurar paths
        service_dir = os.path.dirname(os.path.abspath(__file__))
        sys.path.insert(0, service_dir)
        
        try:
            from src.main import main
            main()
        except Exception as e:
            servicemanager.LogErrorMsg(f"Erro no service: {e}")

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(RFProcessorService)
```

### Script de Instalação Windows

```powershell
# install_windows.ps1

Write-Host "🪟 Instalando RF Processors no Windows..." -ForegroundColor Green

# 1. Verificar privilégios de administrador
if (-NOT ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Error "Execute como Administrador"
    exit 1
}

# 2. Criar diretórios
$InstallPath = "C:\RF_Processors"
New-Item -ItemType Directory -Force -Path $InstallPath
New-Item -ItemType Directory -Force -Path "$InstallPath\logs"
New-Item -ItemType Directory -Force -Path "$InstallPath\data"

# 3. Copiar arquivos
Copy-Item -Recurse -Force "dist\*" $InstallPath

# 4. Instalar Python dependencies
Set-Location $InstallPath
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements_production.txt

# 5. Instalar Windows Service
python windows_service.py install

# 6. Configurar service
sc config RFProcessors start=auto
sc description RFProcessors "Sistema de processamento de dados da Receita Federal"

# 7. Iniciar service
Start-Service RFProcessors

# 8. Verificar status
Get-Service RFProcessors

Write-Host "✅ Instalação concluída!" -ForegroundColor Green
Write-Host "📊 Status: Get-Service RFProcessors" -ForegroundColor Yellow
Write-Host "📋 Logs: Event Viewer > Applications and Services Logs" -ForegroundColor Yellow
```

## 📊 Monitoramento e Health Checks

### Health Check Endpoint

```python
# src/health_check.py
from fastapi import FastAPI, HTTPException
from src.process.base.resource_monitor import ResourceMonitor
from src.process.base.factory import ProcessorFactory
import psutil
import time

app = FastAPI(title="RF Processors Health Check")

@app.get("/health")
async def health_check():
    """Health check básico"""
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "service": "rf-processors"
    }

@app.get("/health/detailed")
async def detailed_health_check():
    """Health check detalhado"""
    try:
        # Verificar recursos
        monitor = ResourceMonitor()
        resources = monitor.get_system_resources_dict()
        
        # Verificar processadores
        registered = ProcessorFactory.get_registered_processors()
        
        # Verificar disco
        disk_usage = psutil.disk_usage('/')
        
        health_data = {
            "status": "healthy",
            "timestamp": time.time(),
            "resources": {
                "cpu_percent": resources['cpu_percent'],
                "memory_percent": resources['memory_percent'],
                "disk_free_gb": disk_usage.free / (1024**3)
            },
            "processors": {
                "registered_count": len(registered),
                "registered_types": registered
            },
            "checks": {
                "cpu_ok": resources['cpu_percent'] < 90,
                "memory_ok": resources['memory_percent'] < 90,
                "disk_ok": disk_usage.free > 10 * (1024**3),  # 10GB
                "processors_ok": len(registered) >= 4
            }
        }
        
        # Verificar se todos os checks passaram
        all_checks_ok = all(health_data["checks"].values())
        if not all_checks_ok:
            health_data["status"] = "degraded"
        
        return health_data
        
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Health check failed: {e}")

@app.get("/metrics")
async def prometheus_metrics():
    """Métricas no formato Prometheus"""
    monitor = ResourceMonitor()
    resources = monitor.get_system_resources_dict()
    
    metrics = f"""
# HELP rf_cpu_percent CPU usage percentage
# TYPE rf_cpu_percent gauge
rf_cpu_percent {resources['cpu_percent']}

# HELP rf_memory_percent Memory usage percentage  
# TYPE rf_memory_percent gauge
rf_memory_percent {resources['memory_percent']}

# HELP rf_processors_registered Number of registered processors
# TYPE rf_processors_registered gauge
rf_processors_registered {len(ProcessorFactory.get_registered_processors())}
"""
    
    return metrics.strip()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
```

### Configuração de Monitoramento

```yaml
# monitoring/prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'rf-processors'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: '/metrics'
    scrape_interval: 30s
```

### Alerting Rules

```yaml
# monitoring/alert_rules.yml
groups:
  - name: rf_processors
    rules:
      - alert: HighCPUUsage
        expr: rf_cpu_percent > 90
        for: 5m
        annotations:
          summary: "Alto uso de CPU"
          description: "CPU usage está em {{ $value }}%"
      
      - alert: HighMemoryUsage
        expr: rf_memory_percent > 90
        for: 5m
        annotations:
          summary: "Alto uso de memória"
          description: "Memory usage está em {{ $value }}%"
      
      - alert: ProcessorDown
        expr: rf_processors_registered < 4
        for: 1m
        annotations:
          summary: "Processadores não registrados"
          description: "Apenas {{ $value }} processadores registrados"
```

## 🔒 Segurança

### Configurações de Segurança

```python
# config/security.py
import os
from pathlib import Path

class SecurityConfig:
    # Diretórios seguros
    ALLOWED_INPUT_DIRS = [
        "/secure/data/input",
        "/opt/rf_data/input"
    ]
    
    ALLOWED_OUTPUT_DIRS = [
        "/secure/data/output", 
        "/opt/rf_data/output"
    ]
    
    # Validações de arquivo
    MAX_FILE_SIZE_MB = 2048  # 2GB
    ALLOWED_FILE_EXTENSIONS = ['.zip', '.csv']
    
    # Recursos
    MAX_WORKERS = 16
    MAX_MEMORY_PERCENT = 85
    
    @staticmethod
    def validate_path(path: str, allowed_dirs: list) -> bool:
        """Valida se path está em diretório permitido"""
        path_obj = Path(path).resolve()
        
        for allowed_dir in allowed_dirs:
            allowed_path = Path(allowed_dir).resolve()
            try:
                path_obj.relative_to(allowed_path)
                return True
            except ValueError:
                continue
        
        return False
    
    @staticmethod
    def validate_file(file_path: str) -> bool:
        """Valida arquivo de entrada"""
        path = Path(file_path)
        
        # Verificar extensão
        if path.suffix.lower() not in SecurityConfig.ALLOWED_FILE_EXTENSIONS:
            return False
        
        # Verificar tamanho
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            if size_mb > SecurityConfig.MAX_FILE_SIZE_MB:
                return False
        
        return True
```

### Configuração de Logs Seguros

```python
# config/logging_config.py
import logging
import logging.handlers
from pathlib import Path

def setup_secure_logging():
    """Configura logging seguro para produção"""
    
    # Criar diretório de logs
    log_dir = Path("/var/log/rf_processors")
    log_dir.mkdir(exist_ok=True, mode=0o750)
    
    # Configurar formatação
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Handler para arquivo (com rotação)
    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / "rf_processors.log",
        maxBytes=100 * 1024 * 1024,  # 100MB
        backupCount=10
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    
    # Handler para erros
    error_handler = logging.handlers.RotatingFileHandler(
        log_dir / "rf_processors_errors.log",
        maxBytes=50 * 1024 * 1024,  # 50MB
        backupCount=5
    )
    error_handler.setFormatter(formatter)
    error_handler.setLevel(logging.ERROR)
    
    # Configurar root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(error_handler)
    
    # Não log para stdout em produção (segurança)
    root_logger.propagate = False
```

## 🔄 Estratégias de Deploy

### Blue-Green Deployment

```bash
#!/bin/bash
# blue_green_deploy.sh

CURRENT_COLOR=$(docker ps --filter "name=rf-processor" --format "table {{.Names}}" | grep -o "blue\|green" | head -1)
NEW_COLOR="blue"

if [ "$CURRENT_COLOR" = "blue" ]; then
    NEW_COLOR="green"
fi

echo "🔄 Deploy Blue-Green: $CURRENT_COLOR -> $NEW_COLOR"

# 1. Deploy nova versão
docker-compose -f docker-compose.$NEW_COLOR.yml up -d

# 2. Aguardar health check
echo "⏳ Aguardando health check..."
for i in {1..30}; do
    if curl -f http://localhost:8080/health > /dev/null 2>&1; then
        echo "✅ Health check OK"
        break
    fi
    echo "⏳ Tentativa $i/30..."
    sleep 10
done

# 3. Trocar tráfego (nginx/load balancer)
echo "🔄 Redirecionando tráfego para $NEW_COLOR"
# ... configurar load balancer

# 4. Parar versão antiga
echo "🛑 Parando versão $CURRENT_COLOR"
docker-compose -f docker-compose.$CURRENT_COLOR.yml down

echo "✅ Deploy concluído!"
```

### Rolling Update

```bash
#!/bin/bash
# rolling_update.sh

echo "🔄 Iniciando rolling update..."

# 1. Atualizar um container por vez
CONTAINERS=$(docker ps --filter "name=rf-processor" --format "{{.Names}}")

for container in $CONTAINERS; do
    echo "🔄 Atualizando $container..."
    
    # Parar container
    docker stop $container
    
    # Atualizar imagem
    docker-compose pull rf-processor
    
    # Iniciar novo container
    docker-compose up -d rf-processor
    
    # Aguardar health check
    sleep 30
    
    echo "✅ $container atualizado"
done

echo "✅ Rolling update concluído!"
```

## 📋 Checklist de Deploy

### Pré-Deploy

- [ ] ✅ Testes passando (100%)
- [ ] ✅ Build de produção criado
- [ ] ✅ Configurações validadas
- [ ] ✅ Backup do sistema atual
- [ ] ✅ Recursos de sistema verificados
- [ ] ✅ Dependências atualizadas
- [ ] ✅ Logs configurados
- [ ] ✅ Monitoramento preparado

### Durante Deploy

- [ ] ✅ Ambiente de produção preparado
- [ ] ✅ Deploy executado conforme estratégia
- [ ] ✅ Health checks validados
- [ ] ✅ Smoke tests executados
- [ ] ✅ Logs funcionando
- [ ] ✅ Métricas coletadas
- [ ] ✅ Performance validada

### Pós-Deploy

- [ ] ✅ Sistema funcionando normalmente
- [ ] ✅ Todos os processadores registrados
- [ ] ✅ Processamento de arquivos testado
- [ ] ✅ Alertas configurados
- [ ] ✅ Documentação atualizada
- [ ] ✅ Equipe notificada
- [ ] ✅ Rollback plan validado

---

**💡 Este guia de deploy garante que o sistema refatorado seja implementado de forma segura, monitorada e otimizada em ambientes de produção, com estratégias de fallback e observabilidade completa.** 