"""
Entidade Cnae para representar dados da Classificação Nacional de Atividades Econômicas.

Esta entidade gerencia dados de CNAE conforme disponibilizados pela Receita Federal,
incluindo validações específicas e métodos utilitários para trabalhar com classificações
econômicas hierárquicas.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Type

try:
    from pydantic import BaseModel, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

from .base import BaseEntity


@dataclass
class Cnae(BaseEntity):
    """
    Entidade para representar dados de CNAE (Classificação Nacional de Atividades Econômicas).
    
    O CNAE é organizado hierarquicamente em:
    - Seção (1 letra)
    - Divisão (2 dígitos)
    - Grupo (3 dígitos)
    - Classe (4 dígitos)
    - Subclasse (7 dígitos)
    
    Attributes:
        id_secao: Identificador da seção (letra A-U)
        desc_secao: Descrição da seção
        id_divisao: Identificador da divisão (2 dígitos)
        desc_divisao: Descrição da divisão
        id_grupo: Identificador do grupo (3 dígitos)
        desc_grupo: Descrição do grupo
        id_classe: Identificador da classe (4 dígitos)
        desc_classe: Descrição da classe
        id_subclasse: Identificador da subclasse (7 dígitos)
        desc_subclasse: Descrição da subclasse
    """
    
    # Campos obrigatórios
    id_secao: str
    desc_secao: str
    id_divisao: int
    desc_divisao: str
    id_grupo: int
    desc_grupo: str
    id_classe: int
    desc_classe: str
    id_subclasse: int
    desc_subclasse: str
    
    # Campos calculados automaticamente
    _validation_errors_cnae: List[str] = field(default_factory=list, init=False)
    _is_valid: Optional[bool] = field(default=None, init=False)
    
    def __post_init__(self):
        """Executado após a inicialização para realizar validações e transformações."""
        super().__post_init__()
        self._validate_cnae()
    
    @classmethod
    def get_column_names(cls) -> List[str]:
        """Retorna lista com os nomes das colunas da entidade."""
        return [
            'id_secao', 'desc_secao', 'id_divisao', 'desc_divisao',
            'id_grupo', 'desc_grupo', 'id_classe', 'desc_classe',
            'id_subclasse', 'desc_subclasse'
        ]
    
    @classmethod
    def get_column_types(cls) -> Dict[str, Type]:
        """Retorna dicionário com os tipos das colunas da entidade."""
        return {
            'id_secao': str,
            'desc_secao': str,
            'id_divisao': int,
            'desc_divisao': str,
            'id_grupo': int,
            'desc_grupo': str,
            'id_classe': int,
            'desc_classe': str,
            'id_subclasse': int,
            'desc_subclasse': str
        }
    
    @classmethod
    def get_transformations(cls) -> List[str]:
        """Retorna lista com as transformações aplicáveis à entidade."""
        return [
            'validate_hierarchy',
            'normalize_descriptions',
            'format_cnae_code',
            'categorize_activity'
        ]
    
    def validate(self) -> bool:
        """Valida os dados da entidade."""
        self._validation_errors.clear()
        self._validation_errors_cnae = []
        
        # Validar seção (deve ser uma letra A-U)
        if not isinstance(self.id_secao, str) or len(self.id_secao) != 1:
            error_msg = "Seção deve ser uma única letra"
            self._validation_errors.append(error_msg)
            self._validation_errors_cnae.append(error_msg)
        elif not self.id_secao.isalpha() or not ('A' <= self.id_secao.upper() <= 'U'):
            error_msg = "Seção deve ser uma letra entre A e U"
            self._validation_errors.append(error_msg)
            self._validation_errors_cnae.append(error_msg)
        
        # Validar divisão (1 ou 2 dígitos)
        if not isinstance(self.id_divisao, int) or not (1 <= self.id_divisao <= 99):
            error_msg = "Divisão deve ter 1 ou 2 dígitos (1-99)"
            self._validation_errors.append(error_msg)
            self._validation_errors_cnae.append(error_msg)
        
        # Validar grupo (2 ou 3 dígitos conforme dados reais)
        if not isinstance(self.id_grupo, int) or not (10 <= self.id_grupo <= 999):
            error_msg = "Grupo deve ter 2 ou 3 dígitos (10-999)"
            self._validation_errors.append(error_msg)
            self._validation_errors_cnae.append(error_msg)
        
        # Validar classe (4 dígitos)
        if not isinstance(self.id_classe, int) or not (1000 <= self.id_classe <= 9999):
            error_msg = "Classe deve ter 4 dígitos (1000-9999)"
            self._validation_errors.append(error_msg)
            self._validation_errors_cnae.append(error_msg)
        
        # Validar subclasse (6 ou 7 dígitos conforme dados reais)
        if not isinstance(self.id_subclasse, int) or not (100000 <= self.id_subclasse <= 9999999):
            error_msg = "Subclasse deve ter 6 ou 7 dígitos (100000-9999999)"
            self._validation_errors.append(error_msg)
            self._validation_errors_cnae.append(error_msg)
        
        # Validar hierarquia (cada nível deve ser logicamente consistente) - mais flexível
        if all(isinstance(x, int) for x in [self.id_divisao, self.id_grupo, self.id_classe, self.id_subclasse]):
            # Validar que a classe começa com o grupo
            if not str(self.id_classe).startswith(str(self.id_grupo)):
                error_msg = f"Classe {self.id_classe} deve começar com grupo {self.id_grupo}"
                self._validation_errors.append(error_msg)
                self._validation_errors_cnae.append(error_msg)
            
            # Validar que a subclasse começa com a classe
            classe_str = str(self.id_classe)
            subclasse_str = str(self.id_subclasse)
            if not subclasse_str.startswith(classe_str):
                error_msg = f"Subclasse {self.id_subclasse} deve começar com classe {self.id_classe}"
                self._validation_errors.append(error_msg)
                self._validation_errors_cnae.append(error_msg)
        
        # Validar descrições
        for campo, descricao in [
            ('seção', self.desc_secao),
            ('divisão', self.desc_divisao),
            ('grupo', self.desc_grupo),
            ('classe', self.desc_classe),
            ('subclasse', self.desc_subclasse)
        ]:
            if not descricao or not descricao.strip():
                error_msg = f"Descrição da {campo} é obrigatória"
                self._validation_errors.append(error_msg)
                self._validation_errors_cnae.append(error_msg)
            elif len(descricao.strip()) < 3:
                error_msg = f"Descrição da {campo} deve ter pelo menos 3 caracteres"
                self._validation_errors.append(error_msg)
                self._validation_errors_cnae.append(error_msg)
        
        self._is_valid = len(self._validation_errors) == 0
        return self._is_valid
    
    def _validate_cnae(self) -> None:
        """Valida os dados do CNAE (método para compatibilidade)."""
        self.validate()
    
    def is_valid(self) -> bool:
        """Retorna True se o CNAE é válido."""
        return self._is_valid if self._is_valid is not None else False
    
    def get_validation_errors(self) -> List[str]:
        """Retorna lista de erros de validação."""
        return self._validation_errors_cnae.copy()
    
    def get_codigo_formatado(self) -> str:
        """
        Retorna o código da subclasse formatado (XXXX-X/XX ou XXX-X/XX).
        
        Returns:
            str: Código CNAE formatado
        """
        if not isinstance(self.id_subclasse, int):
            return str(self.id_subclasse)
        
        codigo_str = str(self.id_subclasse)
        
        # Para códigos de 6 dígitos (formato: XXXX-XX)
        if len(codigo_str) == 6:
            return f"{codigo_str[:4]}-{codigo_str[4:]}"
        # Para códigos de 7 dígitos (formato: XXXX-X/XX)
        elif len(codigo_str) == 7:
            return f"{codigo_str[:4]}-{codigo_str[4]}/{codigo_str[5:]}"
        else:
            # Para outros casos, retornar como está
            return codigo_str
    
    def get_hierarquia_completa(self) -> Dict[str, Any]:
        """
        Retorna a hierarquia completa do CNAE.
        
        Returns:
            Dict: Hierarquia com todos os níveis
        """
        return {
            'secao': {'id': self.id_secao, 'descricao': self.desc_secao},
            'divisao': {'id': self.id_divisao, 'descricao': self.desc_divisao},
            'grupo': {'id': self.id_grupo, 'descricao': self.desc_grupo},
            'classe': {'id': self.id_classe, 'descricao': self.desc_classe},
            'subclasse': {'id': self.id_subclasse, 'descricao': self.desc_subclasse}
        }
    
    def get_categoria_principal(self) -> str:
        """
        Retorna a categoria principal baseada na seção.
        
        Returns:
            str: Categoria principal da atividade econômica
        """
        categorias = {
            'A': 'Agricultura, Pecuária e Pesca',
            'B': 'Indústrias Extrativas',
            'C': 'Indústrias de Transformação',
            'D': 'Eletricidade e Gás',
            'E': 'Água, Esgoto e Resíduos',
            'F': 'Construção',
            'G': 'Comércio e Reparação',
            'H': 'Transporte e Armazenagem',
            'I': 'Alojamento e Alimentação',
            'J': 'Informação e Comunicação',
            'K': 'Atividades Financeiras',
            'L': 'Atividades Imobiliárias',
            'M': 'Atividades Profissionais',
            'N': 'Atividades Administrativas',
            'O': 'Administração Pública',
            'P': 'Educação',
            'Q': 'Saúde Humana e Social',
            'R': 'Artes, Cultura e Esporte',
            'S': 'Outras Atividades de Serviços',
            'T': 'Serviços Domésticos',
            'U': 'Organismos Internacionais'
        }
        
        return categorias.get(self.id_secao.upper(), 'Categoria Desconhecida')
    
    def is_industrial(self) -> bool:
        """
        Verifica se a atividade é industrial.
        
        Returns:
            bool: True se for atividade industrial
        """
        return self.id_secao.upper() in ['B', 'C', 'D']
    
    def is_comercio(self) -> bool:
        """
        Verifica se a atividade é de comércio.
        
        Returns:
            bool: True se for atividade de comércio
        """
        return self.id_secao.upper() == 'G'
    
    def is_servicos(self) -> bool:
        """
        Verifica se a atividade é de serviços.
        
        Returns:
            bool: True se for atividade de serviços
        """
        return self.id_secao.upper() in ['H', 'I', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T']
    
    def is_agropecuaria(self) -> bool:
        """
        Verifica se a atividade é agropecuária.
        
        Returns:
            bool: True se for atividade agropecuária
        """
        return self.id_secao.upper() == 'A'
    
    def get_descricao_completa(self) -> str:
        """
        Retorna a descrição completa da subclasse.
        
        Returns:
            str: Descrição completa da atividade
        """
        return f"{self.get_codigo_formatado()} - {self.desc_subclasse}"
    
    def get_path_hierarquico(self) -> str:
        """
        Retorna o caminho hierárquico completo.
        
        Returns:
            str: Caminho da seção até a subclasse
        """
        return f"{self.id_secao} > {self.id_divisao} > {self.id_grupo} > {self.id_classe} > {self.id_subclasse}"
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Cnae':
        """
        Cria uma instância de Cnae a partir de um dicionário.
        
        Args:
            data: Dicionário com os dados do CNAE
            
        Returns:
            Cnae: Nova instância da entidade
        """
        return cls(
            id_secao=data.get('id_secao', ''),
            desc_secao=data.get('desc_secao', ''),
            id_divisao=data.get('id_divisao'),
            desc_divisao=data.get('desc_divisao', ''),
            id_grupo=data.get('id_grupo'),
            desc_grupo=data.get('desc_grupo', ''),
            id_classe=data.get('id_classe'),
            desc_classe=data.get('desc_classe', ''),
            id_subclasse=data.get('id_subclasse'),
            desc_subclasse=data.get('desc_subclasse', '')
        )
    
    @classmethod
    def from_polars_row(cls, row: Dict[str, Any]) -> 'Cnae':
        """
        Cria uma instância de Cnae a partir de uma linha do Polars.
        
        Args:
            row: Dicionário representando uma linha do DataFrame
            
        Returns:
            Cnae: Nova instância da entidade
        """
        return cls.from_dict(row)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Converte a entidade para um dicionário.
        
        Returns:
            Dict: Dicionário com todos os campos da entidade
        """
        return {
            'id_secao': self.id_secao,
            'desc_secao': self.desc_secao,
            'id_divisao': self.id_divisao,
            'desc_divisao': self.desc_divisao,
            'id_grupo': self.id_grupo,
            'desc_grupo': self.desc_grupo,
            'id_classe': self.id_classe,
            'desc_classe': self.desc_classe,
            'id_subclasse': self.id_subclasse,
            'desc_subclasse': self.desc_subclasse,
            'codigo_formatado': self.get_codigo_formatado(),
            'categoria_principal': self.get_categoria_principal(),
            'is_valid': self.is_valid(),
            'validation_errors': self.get_validation_errors()
        }
    
    def __str__(self) -> str:
        """Representação em string da entidade."""
        status = "✅" if self.is_valid() else "❌"
        codigo_formatado = self.get_codigo_formatado()
        categoria = self.get_categoria_principal()
        return f"{status} CNAE {codigo_formatado}: {self.desc_subclasse[:50]}... [{categoria}]"
    
    def __repr__(self) -> str:
        """Representação técnica da entidade."""
        return f"Cnae(subclasse={self.id_subclasse}, secao='{self.id_secao}', valid={self.is_valid()})"


if PYDANTIC_AVAILABLE:
    class CnaeSchema(BaseModel):
        """Schema Pydantic para validação da entidade Cnae."""
        
        id_secao: str = Field(..., min_length=1, max_length=1, description="Seção (letra A-U)")
        desc_secao: str = Field(..., min_length=3, description="Descrição da seção")
        id_divisao: int = Field(..., ge=10, le=99, description="Divisão (2 dígitos)")
        desc_divisao: str = Field(..., min_length=3, description="Descrição da divisão")
        id_grupo: int = Field(..., ge=100, le=999, description="Grupo (3 dígitos)")
        desc_grupo: str = Field(..., min_length=3, description="Descrição do grupo")
        id_classe: int = Field(..., ge=1000, le=9999, description="Classe (4 dígitos)")
        desc_classe: str = Field(..., min_length=3, description="Descrição da classe")
        id_subclasse: int = Field(..., ge=1000000, le=9999999, description="Subclasse (7 dígitos)")
        desc_subclasse: str = Field(..., min_length=3, description="Descrição da subclasse")
        
        class Config:
            json_schema_extra = {
                "example": {
                    "id_secao": "A",
                    "desc_secao": "Agricultura, pecuária, produção florestal, pesca e aquicultura",
                    "id_divisao": 11,
                    "desc_divisao": "Agricultura, pecuária e serviços relacionados",
                    "id_grupo": 111,
                    "desc_grupo": "Cultivo de cereais",
                    "id_classe": 1113,
                    "desc_classe": "Cultivo de cereais",
                    "id_subclasse": 1113001,
                    "desc_subclasse": "Cultivo de arroz"
                }
            }
        
        @validator('id_secao')
        def validate_secao(cls, v):
            if not v.isalpha() or not ('A' <= v.upper() <= 'U'):
                raise ValueError('Seção deve ser uma letra entre A e U')
            return v.upper()
        
        @validator('id_grupo')
        def validate_grupo_hierarquia(cls, v, values):
            if 'id_divisao' in values and not str(v).startswith(str(values['id_divisao'])):
                raise ValueError('Grupo deve começar com os dígitos da divisão')
            return v
        
        @validator('id_classe')
        def validate_classe_hierarquia(cls, v, values):
            if 'id_grupo' in values and not str(v).startswith(str(values['id_grupo'])):
                raise ValueError('Classe deve começar com os dígitos do grupo')
            return v
        
        @validator('id_subclasse')
        def validate_subclasse_hierarquia(cls, v, values):
            if 'id_classe' in values and not str(v).startswith(str(values['id_classe'])):
                raise ValueError('Subclasse deve começar com os dígitos da classe')
            return v
