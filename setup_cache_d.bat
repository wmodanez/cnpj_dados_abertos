@echo off
echo Configurando cache do pip para unidade D...
pip config set global.cache-dir D:\pip-cache
echo Cache do pip configurado para D:\pip-cache

echo Criando pasta de cache na unidade D...
mkdir D:\pip-cache 2>nul
mkdir D:\temp-python 2>nul

echo Configuração concluída!
echo Para aplicar as variáveis de ambiente temporariamente nesta sessão:
echo set TEMP=D:\temp-python
echo set TMP=D:\temp-python
echo set PYTHONPYCACHEPREFIX=D:\temp-python\pycache

pause 