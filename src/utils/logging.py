import datetime
import logging
import os


class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


class ColoredFormatter(logging.Formatter):
    """Formatação colorida para o console."""

    def format(self, record):
        if record.levelname == 'INFO':
            color = Colors.GREEN
        elif record.levelname == 'WARNING':
            color = Colors.YELLOW
        elif record.levelname == 'ERROR':
            color = Colors.RED
        elif record.levelname == 'CRITICAL':
            color = Colors.RED + Colors.BOLD
        else:
            color = Colors.END

        record.msg = f"{color}{record.msg}{Colors.END}"
        return super().format(record)


def setup_logging():
    """Configura o sistema de logging."""
    if not os.path.exists('logs'):
        os.makedirs('logs')

    log_filename = f'logs/cnpj_process_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    # Configuração do logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Handler para arquivo (sem cores)
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    logger.addHandler(file_handler)

    # Handler para console (com cores)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColoredFormatter(log_format, date_format))
    logger.addHandler(console_handler)

    return logger


def print_header(text: str):
    """Imprime um cabeçalho formatado."""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 50}")
    print(f"{text}")
    print(f"{'=' * 50}{Colors.END}\n")


def print_section(text: str):
    """Imprime uma seção formatada."""
    print(f"\n{Colors.BLUE}{Colors.BOLD}▶ {text}{Colors.END}")


def print_success(text: str):
    """Imprime uma mensagem de sucesso formatada."""
    print(f"{Colors.GREEN}✓ {text}{Colors.END}")


def print_warning(text: str):
    """Imprime uma mensagem de aviso formatada."""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.END}")


def print_error(text: str):
    """Imprime uma mensagem de erro formatada."""
    print(f"{Colors.RED}✗ {text}{Colors.END}")
