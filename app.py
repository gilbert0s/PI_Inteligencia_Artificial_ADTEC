import pandas as pd
import re
import numpy as np
# Certifique-se de que a biblioteca openpyxl está instalada para ler arquivos .xlsx

# Definição dos nomes de arquivos
FILE_NAME = "Tabela CEMIG.xlsx"
OUTPUT_FILE = 'dados_anonimizados_adtec.csv'

# --- 1. Funções de Mascaramento (Inteligência Artificial & Segurança) ---

def mask_name(full_name: str) -> str:
    """
    Função de Mascaramento de Nomes: Mantém o primeiro nome e máscara o restante.
    Aplica o princípio da Anonimização para conformidade com a LGPD.
    """
    if not isinstance(full_name, str) or not full_name.strip():
        return full_name

    parts = full_name.split()
    if not parts:
        return full_name

    # O primeiro nome é mantido como identificador mínimo
    masked_parts = [parts[0]]

    # As palavras subsequentes são mascaradas (primeira letra + asteriscos)
    for part in parts[1:]:
        if len(part) > 0:
            # Mantém a primeira letra e substitui o restante (N-1) por asteriscos
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

    # Padrão RegEx: Captura o primeiro grupo (\d{3}) e o último grupo (\d{2})
    cpf_pattern = r'(\d{3})\.\d{3}\.\d{3}-(\d{2})'
    
    # Verifica se o CPF corresponde exatamente ao padrão
    if re.fullmatch(cpf_pattern, cpf):
        # Aplica o mascaramento
        return re.sub(cpf_pattern, r'\1.***.***-\2', cpf)
    else:
        # Se o padrão não for reconhecido, retorna NaN (mais seguro que retornar o original)
        return np.nan 


# --- 2. Classificação de Colunas (Inteligência Artificial - Heurística Aprimorada) ---

def classify_columns(df: pd.DataFrame) -> dict:
    """
    Classifica as colunas do DataFrame para identificar o tipo de dado pessoal.
    A chave 'NOME' agora retorna uma LISTA de colunas de nomes.
    """
    # Inicializa 'NOME' como uma lista para aceitar múltiplas colunas de nomes (Titular, Proprietário, etc.)
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

    # === 2. CLASSIFICAÇÃO DE NOME (Heurística Aprimorada para Múltiplas Colunas) ===
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


# --- 3. Processamento e Fluxo Principal do Projeto ---

def process_data_and_anonymize(file_path: str, output_path: str):
    """
    Função principal que carrega o arquivo XLSX, aplica a IA/Segurança e salva.
    """
    try:
        # 1. Carregamento dos Dados (Corrigido para .xlsx)
        print(f"Lendo arquivo Excel: {file_path}...")
        df = pd.read_excel(file_path) 

        # 2. Classificação de Colunas pela IA
        print("Classificando colunas para identificar dados sensíveis (Heurística de IA)...")
        column_map = classify_columns(df)
        
        # 3. Validação da Classificação
        # Verifica se pelo menos o CPF e ALGUMA coluna de NOME foram encontradas
        if 'CPF' not in column_map or not column_map.get('NOME'):
            print("Erro: A IA não conseguiu identificar as colunas de CPF e/ou NOME no arquivo.")
            print(f"Colunas identificadas: {column_map}")
            return

        # 4. Aplicação do Mascaramento
        cpf_col = column_map['CPF']
        name_cols = column_map['NOME'] # Agora é uma lista com Titular, Proprietário, etc.
        
        print(f"Aplicando mascaramento em: CPF ({cpf_col}) e Nomes ({', '.join(name_cols)})...")
        
        # Cria as colunas mascaradas para CPF
        df['CPF Mascarado'] = df[cpf_col].apply(mask_cpf)
        
        # Cria as colunas mascaradas para TODOS os nomes identificados
        for name_col in name_cols:
            df[f'{name_col} Mascarado'] = df[name_col].apply(mask_name)

        # 5. Geração de Logs de Segurança
        total_rows = len(df)
        cpf_masked_count = df['CPF Mascarado'].dropna().count() # Usando .count() para ser mais preciso
        
        print(f"-> Logs de Segurança: {total_rows} registros processados. {cpf_masked_count} CPFs válidos mascarados.")
        
        # 6. Salvamento do Resultado (Risco Mitigado)
        output_cols = [col for col in df.columns if 'Mascarado' in col or col in column_map.values()]
        
        df[output_cols].to_csv(output_path, index=False, encoding='utf-8')

        print(f"\nProcessamento concluído com sucesso!")
        print(f"Risco de exposição de dados mitigado (LGPD) e salvo em: {output_path}")
        
        # 7. Amostra para Prova de Conceito (Ajuste para mostrar Proprietário e Titular)
        # Monta a lista de colunas para a amostra dinamicamente
        display_cols = []
        if 'Proprietário' in name_cols:
            display_cols.extend(['Proprietário', 'Proprietário Mascarado'])
        if 'Titular' in name_cols:
            display_cols.extend(['Titular', 'Titular Mascarado'])
        
        # Garante que CPF está no final da amostra
        display_cols.extend(['CPF', 'CPF Mascarado'])

        print("\nAmostra dos Dados Anonimizados para Prova de Conceito:")
        print(df[display_cols].head().to_markdown(index=False))

    except FileNotFoundError:
        print(f"\nErro: O arquivo de entrada '{file_path}' não foi encontrado. Verifique a nomenclatura e o caminho.")
    except Exception as e:
        print(f"\nOcorreu um erro inesperado durante o processamento: {e}")


# --- 4. Ponto de Entrada do Programa ---

if __name__ == "__main__":
    process_data_and_anonymize(FILE_NAME, OUTPUT_FILE)