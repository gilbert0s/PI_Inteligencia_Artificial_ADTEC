import pandas as pd
import re
import numpy as np
from flask import Flask, render_template_string, request, redirect, url_for, session
from io import StringIO
from math import ceil

# Importação da biblioteca para leitura de Excel (obrigatório para .xlsx)
# Certifique-se de que a biblioteca openpyxl está instalada: pip install openpyxl

# Inicialização do Aplicativo Flask
app = Flask(__name__)
# Chave secreta necessária para usar sessões (mantém o login ativo)
app.secret_key = 'chave_super_secreta_para_sessao' 

# Definições Finais
FILE_NAME = "Tabela CEMIG.xlsx"
OUTPUT_FILE = 'dados_anonimizados_adtec.csv'

# --- 0. Autenticação Simples (Simulação de Segurança) ---
# Define os níveis de acesso: 'full' (completo) e 'masked' (anonimizado)
USERS = {
    "admin": {"password": "senha_alto_padrao", "access_level": "full"}, # Acesso Total
    "user": {"password": "senha_convencional", "access_level": "masked"} # Acesso Anonimizado
}

# Variáveis globais para armazenar os DataFrames após a leitura inicial
DF_ORIGINAL = None
DF_MASKED = None
COLUMN_MAP = None
NAME_COLS = None

# --- 1. Funções de Mascaramento (Inteligência Artificial & Segurança) ---

def mask_name(full_name: str) -> str:
    """
    Função de Mascaramento de Nomes: Mantém o primeiro nome e máscara o restante.
    """
    if not isinstance(full_name, str) or not full_name.strip():
        return full_name

    parts = full_name.split()
    if not parts:
        return full_name

    masked_parts = [parts[0]]

    for part in parts[1:]:
        if len(part) > 0:
            masked_part = part[0] + '*' * (len(part) - 1)
            masked_parts.append(masked_part)
        else:
            masked_parts.append(part)

    return ' '.join(masked_parts)


def mask_cpf(cpf: str) -> str:
    """
    Função de Mascaramento de CPF: Utiliza Expressões Regulares (Reconhecimento de Padrão)
    para identificar o formato e aplicar o mascaramento, retornando NaN se inválido.
    """
    if not isinstance(cpf, str):
        return cpf

    cpf_pattern = r'(\d{3})\.\d{3}\.\d{3}-(\d{2})'
    
    if re.fullmatch(cpf_pattern, cpf):
        return re.sub(cpf_pattern, r'\1.***.***-\2', cpf)
    else:
        return np.nan 


# --- 2. Classificação de Colunas (Inteligência Artificial - Heurística Aprimorada) ---

def classify_columns(df: pd.DataFrame) -> dict:
    """
    Classifica as colunas do DataFrame para identificar o tipo de dado pessoal.
    Utiliza Heurística Aprimorada para garantir múltiplos Nomes e evitar Endereços.
    """
    column_mapping = {'NOME': []}
    cpf_regex = r'\d{3}\.\d{3}\.\d{3}-\d{2}'
    
    candidate_cols = [col for col in df.columns if df[col].dtype == object]
    
    # === 1. CLASSIFICAÇÃO DE CPF (Máxima Prioridade por RegEx) ===
    cpf_found = None
    for col in candidate_cols:
        series = df[col].dropna().astype(str)
        if series.empty: continue
            
        match_rate = series.apply(lambda x: bool(re.fullmatch(cpf_regex, x))).sum() / len(series)

        if match_rate >= 0.80:
            column_mapping['CPF'] = col
            cpf_found = col
            break 
    
    if cpf_found:
        candidate_cols.remove(cpf_found)

    # === 2. CLASSIFICAÇÃO DE NOME (Heurística Aprimorada) ===
    name_keywords = ['proprietário', 'titular', 'nome', 'contratante']
    
    for col in candidate_cols:
        col_lower = col.lower()
        
        # 2.1. Prioridade por Nomenclatura (Regra de Negócio)
        if any(keyword in col_lower for keyword in name_keywords):
            column_mapping['NOME'].append(col)
            continue
        
        # 2.2. Heurística de Conteúdo (Verificação de Endereço/Outros)
        else:
            series = df[col].dropna().astype(str)
            if series.empty: continue
            
            # Regra de Exclusão: Se a coluna contém números em mais de 20% das linhas, não é NOME.
            has_numbers_rate = series.apply(lambda x: bool(re.search(r'\d', x))).sum() / len(series)

            if has_numbers_rate < 0.20:
                 # Regra de Inclusão: Se a coluna tem pelo menos 2 palavras e a primeira linha está em Title Case.
                 if len(series.iloc[0].split()) >= 2 and series.iloc[0].istitle(): 
                    column_mapping['NOME'].append(col)
                    
    return column_mapping


# --- 3. Carregamento de Dados (Global) ---

def load_and_process_data():
    """Carrega o arquivo, processa, mascara e armazena os DataFrames originais e mascarados."""
    global DF_ORIGINAL, DF_MASKED, COLUMN_MAP, NAME_COLS
    
    # Verifica se já carregou para evitar recarregar a cada requisição web
    if DF_ORIGINAL is not None and not DF_ORIGINAL.empty:
        return
        
    try:
        df = pd.read_excel(FILE_NAME)
        
        # Realiza a classificação para obter o mapeamento das colunas
        global COLUMN_MAP 
        COLUMN_MAP = classify_columns(df)
        
        cpf_col = COLUMN_MAP.get('CPF')
        global NAME_COLS
        NAME_COLS = COLUMN_MAP.get('NOME', [])
        
        # Salva o original (Para acesso full)
        DF_ORIGINAL = df.copy()
        
        # Cria a cópia mascarada (Para acesso masked)
        DF_MASKED = df.copy()
        
        # Aplica o mascaramento no DF_MASKED
        DF_MASKED['CPF Mascarado'] = DF_MASKED[cpf_col].apply(mask_cpf)
        for name_col in NAME_COLS:
            DF_MASKED[f'{name_col} Mascarado'] = DF_MASKED[name_col].apply(mask_name)
            
        print("Dados carregados e processados com sucesso para a aplicação web!")
        
    except Exception as e:
        print(f"Erro fatal ao carregar dados: {e}")
        DF_ORIGINAL = pd.DataFrame()
        DF_MASKED = pd.DataFrame()


# --- Rotas da Aplicação Web (Paginação e Segurança) ---

@app.route('/', methods=['GET', 'POST'])
def login():
    """Rota de Login para controle de acesso."""
    # ... (código de login omitido - permanece o mesmo)
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        if username in USERS and USERS[username]['password'] == password:
            session['logged_in'] = True
            session['access_level'] = USERS[username]['access_level']
            session['username'] = username
            return redirect(url_for('data_view'))
        else:
            error = 'Usuário ou senha inválidos'
            return render_template_string(LOGIN_PAGE, error=error)
            
    return render_template_string(LOGIN_PAGE, error=None)

@app.route('/logout')
def logout():
    """Rota para fazer logout."""
    session.pop('logged_in', None)
    session.pop('access_level', None)
    session.pop('username', None)
    return redirect(url_for('login'))

@app.route('/data')
def data_view():
    """Rota de Visualização de Dados com Paginação e Controle de Acesso."""
    if not session.get('logged_in'):
        return redirect(url_for('login'))
        
    load_and_process_data() # Garante que os dados estão carregados
    
    access_level = session.get('access_level')
    
    # 1. Define o DataFrame a ser exibido
    if access_level == 'full':
        df_display = DF_ORIGINAL
        title = "Dados Originais (Acesso de Alto Padrão - Risco Alto)"
    else:
        df_display = DF_MASKED
        title = "Dados Anonimizados (Acesso Convencional - LGPD Conformidade)"
        
    
    # Lógica de Paginação
    page = request.args.get('page', 1, type=int)
    per_page = 15
    total_pages = ceil(len(df_display) / per_page)
    
    start = (page - 1) * per_page
    end = start + per_page
    
    paginated_df = df_display.iloc[start:end]
    
    # 2. Lógica de Filtragem de Colunas (A CORREÇÃO FINAL DE SEGURANÇA)
    if access_level == 'masked':
        # Colunas originais sensíveis: o que DEVE ser escondido
        SENSITIVE_ORIGINALS = COLUMN_MAP.get('NOME', []) + [COLUMN_MAP.get('CPF')]
        
        cols_to_display = []
        
        # Adiciona colunas MASACARADAS (elas são as substitutas seguras)
        for name in NAME_COLS:
             cols_to_display.append(f'{name} Mascarado')
             
        cols_to_display.append(f'{COLUMN_MAP["CPF"]} Mascarado')
        
        # Adiciona colunas NÃO-SENSÍVEIS (Endereço, Instalação, etc.)
        for col in paginated_df.columns:
             # EXCLUI qualquer coluna que seja original sensível E colunas mascaradas já adicionadas
             if col not in SENSITIVE_ORIGINALS and 'Mascarado' not in col:
                 cols_to_display.append(col)

        # Filtra o DataFrame para apenas as colunas aprovadas
        paginated_df = paginated_df[[col for col in cols_to_display if col in paginated_df.columns]]
    
    # 3. Renderiza a Tabela
    table_html = paginated_df.to_html(classes='data', index=False)
    
    return render_template_string(DATA_VIEW_PAGE, 
                                  title=title,
                                  table=table_html,
                                  page=page,
                                  total_pages=total_pages,
                                  username=session.get('username'),
                                  access_level=access_level)

# --- Templates HTML (Omitidos, permanecem os mesmos) ---

LOGIN_PAGE = """
<!doctype html>
<title>Login - Projeto PI (IA/Segurança)</title>
<style>body {font-family: Arial, sans-serif; background: #f4f4f4; text-align: center; padding-top: 50px;} form {background: white; padding: 20px; border-radius: 8px; display: inline-block; box-shadow: 0 0 10px rgba(0,0,0,0.1);}</style>
<h2>Sistema de Gestão de Dados - ADTEC</h2>
{% if error %} <p style="color: red;">{{ error }}</p> {% endif %}
<form method="post">
    <p>Nível de Acesso: Alto Padrão (admin) ou Convencional (user)</p>
    <input type="text" name="username" placeholder="Usuário" required><br><br>
    <input type="password" name="password" placeholder="Senha" required><br><br>
    <input type="submit" value="Acessar Dados">
</form>
"""

DATA_VIEW_PAGE = """
<!doctype html>
<title>{{ title }}</title>
<style>
    body {font-family: Arial, sans-serif; margin: 20px;}
    h2 {color: #333;}
    .header {display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;}
    .user-info {font-size: 14px; color: #555;}
    table {width: 100%; border-collapse: collapse; margin-top: 20px;}
    th, td {border: 1px solid #ddd; padding: 8px; text-align: left;}
    th {background-color: #4CAF50; color: white;}
    .pagination a {margin: 0 5px; text-decoration: none; padding: 5px 10px; border: 1px solid #ccc; border-radius: 4px;}
    .pagination span {margin: 0 5px; padding: 5px 10px; background-color: #eee; border-radius: 4px;}
</style>
<div class="header">
    <h2>{{ title }}</h2>
    <div class="user-info">
        Usuário: <b>{{ username }}</b> (Nível: {{ access_level }}) | <a href="{{ url_for('logout') }}">Sair</a>
    </div>
</div>

<p>Página {{ page }} de {{ total_pages }}</p>

{{ table | safe }}

<div class="pagination" style="margin-top: 20px;">
    {% if page > 1 %}
        <a href="{{ url_for('data_view', page=page - 1) }}">Anterior</a>
    {% endif %}
    <span>{{ page }}</span>
    {% if page < total_pages %}
        <a href="{{ url_for('data_view', page=page + 1) }}">Próximo</a>
    {% endif %}
</div>
<p style="margin-top: 30px; font-size: 12px; color: #888;">
    Demonstração de Segurança - PI UNIFEOB. Os dados anonimizados garantem a conformidade com a LGPD para acessos convencionais.
</p>
"""

if __name__ == '__main__':
    # O debug é ativado para facilitar o desenvolvimento
    app.run(debug=True)