"""
Entidade Municipio para representar dados de municípios brasileiros.

Esta entidade gerencia dados de municípios conforme disponibilizados pela Receita Federal,
incluindo validações específicas e métodos utilitários para trabalhar com dados municipais.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Type
import polars as pl

try:
    from pydantic import BaseModel, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

from .base import BaseEntity


@dataclass
class Municipio(BaseEntity):
    """
    Entidade para representar dados de municípios brasileiros.
    
    Attributes:
        codigo: Código do município (7 dígitos)
        nome: Nome do município
        uf: Código numérico da UF
        sigla_uf: Sigla da UF (2 letras)
        latitude: Latitude em formato texto
        latitudegm: Latitude em graus/minutos/segundos
        longitude: Longitude em formato texto
        longitudeg: Longitude em graus/minutos/segundos
        cod_mn_dados_abertos: Código do município nos dados abertos
    """
    
    # Campos obrigatórios
    codigo: int
    nome: str
    uf: int
    sigla_uf: str
    
    # Campos opcionais de geolocalização
    latitude: Optional[str] = None
    latitudegm: Optional[str] = None
    longitude: Optional[str] = None
    longitudeg: Optional[str] = None
    cod_mn_dados_abertos: Optional[int] = None
    
    # Campos calculados automaticamente
    _validation_errors_municipio: List[str] = field(default_factory=list, init=False)
    _is_valid: Optional[bool] = field(default=None, init=False)
    
    def __post_init__(self):
        """Executado após a inicialização para realizar validações e transformações."""
        super().__post_init__()
        self._validate_municipio()
    
    @classmethod
    def get_column_names(cls) -> List[str]:
        """Retorna lista com os nomes das colunas da entidade."""
        return [
            'codigo', 'nome', 'uf', 'sigla_uf', 'latitude', 
            'latitudegm', 'longitude', 'longitudeg', 'cod_mn_dados_abertos'
        ]
    
    @classmethod
    def get_column_types(cls) -> Dict[str, Type]:
        """Retorna dicionário com os tipos das colunas da entidade."""
        return {
            'codigo': int,
            'nome': str,
            'uf': int,
            'sigla_uf': str,
            'latitude': str,
            'latitudegm': str,
            'longitude': str,
            'longitudeg': str,
            'cod_mn_dados_abertos': int
        }
    
    @classmethod
    def get_transformations(cls) -> List[str]:
        """Retorna lista com as transformações aplicáveis à entidade."""
        return [
            'normalize_coordinates',
            'validate_uf_consistency',
            'format_municipality_name'
        ]
    
    def validate(self) -> bool:
        """Valida os dados da entidade."""
        self._validation_errors.clear()
        self._validation_errors_municipio = []
        
        # Validar código do município (7 dígitos)
        if not isinstance(self.codigo, int) or not (1000000 <= self.codigo <= 9999999):
            self._validation_errors.append("Código do município deve ter 7 dígitos")
            self._validation_errors_municipio.append("Código do município deve ter 7 dígitos")
        
        # Validar nome
        if not self.nome or not self.nome.strip():
            self._validation_errors.append("Nome do município é obrigatório")
            self._validation_errors_municipio.append("Nome do município é obrigatório")
        elif len(self.nome.strip()) < 2:
            self._validation_errors.append("Nome do município deve ter pelo menos 2 caracteres")
            self._validation_errors_municipio.append("Nome do município deve ter pelo menos 2 caracteres")
        
        # Validar UF numérica (códigos IBGE válidos)
        ufs_validas = list(range(11, 18)) + list(range(21, 30)) + list(range(31, 36)) + \
                     list(range(41, 44)) + [50, 51, 52, 53]
        if self.uf not in ufs_validas:
            self._validation_errors.append(f"Código UF inválido: {self.uf}")
            self._validation_errors_municipio.append(f"Código UF inválido: {self.uf}")
        
        # Validar sigla UF
        ufs_siglas = {
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
            'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN',
            'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        }
        if self.sigla_uf not in ufs_siglas:
            self._validation_errors.append(f"Sigla UF inválida: {self.sigla_uf}")
            self._validation_errors_municipio.append(f"Sigla UF inválida: {self.sigla_uf}")
        
        # Validar consistência entre código UF e sigla
        mapeamento_uf = {
            11: 'RO', 12: 'AC', 13: 'AM', 14: 'RR', 15: 'PA', 16: 'AP', 17: 'TO',
            21: 'MA', 22: 'PI', 23: 'CE', 24: 'RN', 25: 'PB', 26: 'PE', 27: 'AL', 28: 'SE', 29: 'BA',
            31: 'MG', 32: 'ES', 33: 'RJ', 35: 'SP',
            41: 'PR', 42: 'SC', 43: 'RS',
            50: 'MS', 51: 'MT', 52: 'GO', 53: 'DF'
        }
        if self.uf in mapeamento_uf and mapeamento_uf[self.uf] != self.sigla_uf:
            error_msg = f"Inconsistência: UF {self.uf} não corresponde à sigla {self.sigla_uf}"
            self._validation_errors.append(error_msg)
            self._validation_errors_municipio.append(error_msg)
        
        self._is_valid = len(self._validation_errors) == 0
        return self._is_valid
    
    def _validate_municipio(self) -> None:
        """Valida os dados do município (método para compatibilidade)."""
        self.validate()
    
    def is_valid(self) -> bool:
        """Retorna True se o município é válido."""
        return self._is_valid if self._is_valid is not None else False
    
    def get_validation_errors(self) -> List[str]:
        """Retorna lista de erros de validação."""
        return self._validation_errors_municipio.copy()
    
    def is_capital(self) -> bool:
        """
        Verifica se o município é uma capital.
        
        Returns:
            bool: True se for uma capital
        """
        capitais = {
            'Rio Branco', 'Maceió', 'Macapá', 'Manaus', 'Salvador', 'Fortaleza',
            'Brasília', 'Vitória', 'Goiânia', 'São Luís', 'Cuiabá', 'Campo Grande',
            'Belo Horizonte', 'Belém', 'João Pessoa', 'Curitiba', 'Recife',
            'Teresina', 'Rio de Janeiro', 'Natal', 'Porto Alegre', 'Porto Velho',
            'Boa Vista', 'Florianópolis', 'São Paulo', 'Aracaju', 'Palmas'
        }
        return self.nome in capitais
    
    def get_regiao(self) -> str:
        """
        Retorna a região do município baseado na UF.
        
        Returns:
            str: Nome da região (Norte, Nordeste, Centro-Oeste, Sudeste, Sul)
        """
        regioes = {
            'Norte': [11, 12, 13, 14, 15, 16, 17],
            'Nordeste': [21, 22, 23, 24, 25, 26, 27, 28, 29],
            'Sudeste': [31, 32, 33, 35],
            'Sul': [41, 42, 43],
            'Centro-Oeste': [50, 51, 52, 53]
        }
        
        for regiao, ufs in regioes.items():
            if self.uf in ufs:
                return regiao
        
        return "Desconhecida"
    
    def get_nome_completo(self) -> str:
        """
        Retorna o nome completo do município com a UF.
        
        Returns:
            str: Nome do município seguido da sigla da UF
        """
        return f"{self.nome}/{self.sigla_uf}"
    
    def has_coordinates(self) -> bool:
        """
        Verifica se o município possui coordenadas geográficas.
        
        Returns:
            bool: True se possui latitude e longitude
        """
        return (self.latitude is not None and self.longitude is not None) or \
               (self.latitudegm is not None and self.longitudeg is not None)
    
    def get_coordinates_dict(self) -> Dict[str, Any]:
        """
        Retorna um dicionário com as coordenadas disponíveis.
        
        Returns:
            Dict: Dicionário com as coordenadas disponíveis
        """
        coords = {}
        if self.latitude:
            coords['latitude'] = self.latitude
        if self.longitude:
            coords['longitude'] = self.longitude
        if self.latitudegm:
            coords['latitude_gms'] = self.latitudegm
        if self.longitudeg:
            coords['longitude_gms'] = self.longitudeg
        
        return coords
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Municipio':
        """
        Cria uma instância de Municipio a partir de um dicionário.
        
        Args:
            data: Dicionário com os dados do município
            
        Returns:
            Municipio: Nova instância da entidade
        """
        return cls(
            codigo=data.get('codigo'),
            nome=data.get('nome', ''),
            uf=data.get('uf'),
            sigla_uf=data.get('sigla_uf', ''),
            latitude=data.get('latitude'),
            latitudegm=data.get('latitudegm'),
            longitude=data.get('longitude'),
            longitudeg=data.get('longitudeg'),
            cod_mn_dados_abertos=data.get('cod_mn_dados_abertos')
        )
    
    @classmethod
    def from_polars_row(cls, row: Dict[str, Any]) -> 'Municipio':
        """
        Cria uma instância de Municipio a partir de uma linha do Polars.
        
        Args:
            row: Dicionário representando uma linha do DataFrame
            
        Returns:
            Municipio: Nova instância da entidade
        """
        return cls.from_dict(row)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Converte a entidade para um dicionário.
        
        Returns:
            Dict: Dicionário com todos os campos da entidade
        """
        return {
            'codigo': self.codigo,
            'nome': self.nome,
            'uf': self.uf,
            'sigla_uf': self.sigla_uf,
            'latitude': self.latitude,
            'latitudegm': self.latitudegm,
            'longitude': self.longitude,
            'longitudeg': self.longitudeg,
            'cod_mn_dados_abertos': self.cod_mn_dados_abertos,
            'is_valid': self.is_valid(),
            'validation_errors': self.get_validation_errors()
        }
    
    def __str__(self) -> str:
        """Representação em string da entidade."""
        status = "✅" if self.is_valid() else "❌"
        return f"{status} Município {self.codigo}: {self.get_nome_completo()}"
    
    def __repr__(self) -> str:
        """Representação técnica da entidade."""
        return f"Municipio(codigo={self.codigo}, nome='{self.nome}', sigla_uf='{self.sigla_uf}', valid={self.is_valid()})"


if PYDANTIC_AVAILABLE:
    class MunicipioSchema(BaseModel):
        """Schema Pydantic para validação da entidade Municipio."""
        
        codigo: int = Field(..., ge=1000000, le=9999999, description="Código do município (7 dígitos)")
        nome: str = Field(..., min_length=2, max_length=100, description="Nome do município")
        uf: int = Field(..., ge=11, le=53, description="Código numérico da UF")
        sigla_uf: str = Field(..., min_length=2, max_length=2, description="Sigla da UF")
        latitude: Optional[str] = Field(None, description="Latitude em formato texto")
        latitudegm: Optional[str] = Field(None, description="Latitude em graus/minutos/segundos")
        longitude: Optional[str] = Field(None, description="Longitude em formato texto")
        longitudeg: Optional[str] = Field(None, description="Longitude em graus/minutos/segundos")
        cod_mn_dados_abertos: Optional[int] = Field(None, description="Código nos dados abertos")
        
        class Config:
            json_schema_extra = {
                "example": {
                    "codigo": 3550308,
                    "nome": "São Paulo",
                    "uf": 35,
                    "sigla_uf": "SP",
                    "latitude": "-23.5505199",
                    "longitude": "-46.6333094"
                }
            }
        
        @validator('sigla_uf')
        def validate_sigla_uf(cls, v):
            ufs_validas = {
                'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
                'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN',
                'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
            }
            if v not in ufs_validas:
                raise ValueError(f'Sigla UF inválida: {v}')
            return v
