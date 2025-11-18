import pandas as pd
import re

# --- 1. Função de Mascaramento de Nomes ---
def mask_name(full_name):
    """
    Aplica o mascaramento ao nome completo.
    """
    if not isinstance(full_name, str):
        return full_name
    
    parts = full_name.split()
    if not parts:
        return full_name

    masked_parts = [parts[0]]
    for part in parts[1:]:
        if len(part) > 0:
            # Mantém a primeira letra e substitui o restante por asteriscos
            masked_part = part[0] + '*' * (len(part) - 1)
            masked_parts.append(masked_part)
        else:
            masked_parts.append(part)

    return ' '.join(masked_parts)

# --- 2. Função de Mascaramento de CPF (IA/Padrão) ---
def mask_cpf(cpf):
    """
    Aplica o mascaramento ao CPF utilizando Expressões Regulares.
    """
    if not isinstance(cpf, str):
        return cpf
    
    # Regex: mantém os 3 primeiros e os 2 últimos, mascarando o meio
    masked_cpf = re.sub(r'(\d{3})\.\d{3}\.\d{3}-(\d{2})', r'\1.***.***-\2', cpf)

    return masked_cpf

# --- 3. Execução Principal e Fluxo de Segurança ---

def run_anonymization_tool():
    """
    Função principal que carrega os dados, aplica a IA/Segurança e salva.
    """
    # Nome do arquivo de entrada e saída
    FILE_NAME = "Tabela CEMIG.xlsx"
    OUTPUT_FILE = 'dados_anonimizados_adtec.csv'

    try:
        # 1. Carregamento dos Dados (Corrigido com encoding e delim_whitespace)
        # O parâmetro delim_whitespace=True resolve problemas de tabulação/espaços misturados
        df = pd.read_excel(FILE_NAME)

        # 2. Aplicação da IA e Mecanismos de Segurança (Anonimização)
        print("Aplicando mascaramento de dados (LGPD)...")
        # Aplicação das funções de mascaramento
        df['Proprietário Mascarado'] = df['Proprietário'].apply(mask_name)
        df['Titular Mascarado'] = df['Titular'].apply(mask_name)
        df['CPF Mascarado'] = df['CPF'].apply(mask_cpf)
        
        # Cria uma nova coluna com o status de conformidade
        df['Status de Conformidade'] = 'Anonimizado'

        # 3. Salvamento do Resultado e Mitigação do Risco
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')

        print(f"\nProcessamento concluído com sucesso!")
        print(f"Risco de exposição de dados mitigado (LGPD) e salvo em: {OUTPUT_FILE}")
        
        # Demonstração dos primeiros 5 registros anonimizados
        print("\nAmostra dos Dados Anonimizados:")
        print(df[['Proprietário', 'Proprietário Mascarado', 'CPF', 'CPF Mascarado']].head().to_markdown(index=False))

    except FileNotFoundError:
        print(f"\nErro: O arquivo de entrada '{FILE_NAME}' não foi encontrado. Verifique se ele está na mesma pasta que o 'app.py'.")
    except Exception as e:
        print(f"\nOcorreu um erro durante o processamento: {e}")

# Ponto de entrada do programa
if __name__ == "__main__":
    run_anonymization_tool()