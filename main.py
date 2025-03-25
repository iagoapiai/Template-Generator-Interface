import customtkinter as ctk
import boto3
import requests
import sqlalchemy
import regex as re
import pandas as pd
import decimal
import boto3.dynamodb.types
import webbrowser
import os

DATABASE_URL = "Confidential"
engine = sqlalchemy.create_engine(DATABASE_URL)

session = boto3.session.Session(
    aws_access_key_id='Confidential',
    aws_secret_access_key='Confidential',
    region_name='Confidential')

class TemplateSelectorApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Zendesk Templates")
        self.geometry("500x500")
        self.resizable(False, False)
        
        self.label = ctk.CTkLabel(self, text="Selecione o template:", font=("Arial", 16))
        self.label.pack(pady=5)
        
        self.templates = [
            "‎ ",
            "Ocorrência",
            "Mudança de Comportamento",
            "Temperatura Elevada",
            "Conectividade Bolt"
        ]
        
        self.listbox = ctk.CTkComboBox(self, values=self.templates, command=self.show_inputs, width=218, justify="center")
        self.listbox.pack(pady=5)

        
        self.inputs_frame = ctk.CTkFrame(self)
        self.inputs_frame.pack(pady=10, fill="both", expand=True)
        
        self.message_label = None
        self.loading_label = None  

        self.dynamic_input = None
        self.selected_template = None

    def clear_inputs(self):
        for widget in self.inputs_frame.winfo_children():
            widget.destroy()

    def show_inputs(self, selected_template):
        self.clear_inputs()
        self.selected_template = selected_template 

        if selected_template == "‎":
            label = None

        elif selected_template == "Ocorrência":
            label = ctk.CTkLabel(self.inputs_frame, text="Digite o n° da ocorrência:", font=("Arial", 14))
            label.pack(pady=5)
            self.dynamic_input = ctk.CTkEntry(self.inputs_frame, width=100, justify="center")
            self.dynamic_input.pack(pady=5)
        
        elif selected_template == "Mudança de Comportamento":
            label = ctk.CTkLabel(self.inputs_frame, text="Digite o n° do equipamento:", font=("Arial", 14))
            label.pack(pady=5)
            self.dynamic_input = ctk.CTkEntry(self.inputs_frame, width=100, justify="center")
            self.dynamic_input.pack(pady=5)
        
        elif selected_template == "Temperatura Elevada":
            label = ctk.CTkLabel(self.inputs_frame, text="Digite o n° da ocorrência:", font=("Arial", 14))
            label.pack(pady=5)
            self.dynamic_input = ctk.CTkEntry(self.inputs_frame, width=100, justify="center")
            self.dynamic_input.pack(pady=5)
        
        elif selected_template == "Conectividade Bolt":
            label = ctk.CTkLabel(self.inputs_frame, text="Digite o QR Code do bolt:", font=("Arial", 14))
            label.pack(pady=5)
            self.dynamic_input = ctk.CTkEntry(self.inputs_frame, width=250, justify="center")
            self.dynamic_input.pack(pady=5)

        if selected_template != "‎ ":
            self.submit_button = ctk.CTkButton(
                self.inputs_frame, 
                text="Gerar Template",
                command=self.handle_main_code
            )
            self.submit_button.pack(pady=10)

    def show_loading(self):
            if not self.loading_label:
                self.loading_label = ctk.CTkLabel(
                    self.inputs_frame,
                    text="Carregando...",
                    font=("Arial", 14, "italic"),
                    text_color="orange"
                )
                self.loading_label.pack(pady=5)

    def hide_loading(self):
        if self.loading_label:
            self.loading_label.destroy()
            self.loading_label = None

    def handle_main_code(self):
        if self.dynamic_input:
            user_input = self.dynamic_input.get()

            self.show_loading()

            self.after(100, lambda: self.run_main_code(self.selected_template, user_input))

            if self.submit_button:
                self.submit_button.destroy()

    def run_main_code(self, template, user_input):
        try:
            self.Main_Code(template, user_input)
        finally:
            self.hide_loading()


    def Main_Code(self, template, user_input):

            username = "Confidential"
            password = "Confidential"
            client_id = "Confidential"
            region = "Confidential"
            data_url = "Confidential"
            id_whatsapp = "Confidential"

            def clean_string(value):
                if isinstance(value, str):
                    return re.sub(r'[^a-zA-Z0-9À-ÿ\s\-]', '', value)
                return value
            
            def clean_and_title(name):
                cleaned_name = re.sub(r"[^\p{L}\p{N}\s]", " ", name)
                return re.sub(r"\s+", " ", cleaned_name).strip().title()

            def script_bolt():
                boto3.dynamodb.types.DYNAMODB_CONTEXT = decimal.Context(prec=100)

                sql_query = """
                select c.name as "Empresa",
                c.id as "companyId",
                f.name as "Unidade",
                f.id as "facilityId",
                r.name as "Representante"
                from "tbFacility" f
                join "tbCompany" c on c.id = f."companyId"
                left join "tbRepresentative" r on r.id = c."representativeId"
                where f."deletedAt" is null 
                and c."deletedAt" is null
                and c."skipMetrics" is false"""

                df_banco = pd.read_sql_query(sql_query, engine)

                dyna = session.resource('dynamodb')
                table = dyna.Table('Confidential')
                table_scan = table.scan(Select = "ALL_ATTRIBUTES")
                df_bruto = pd.json_normalize(table_scan['Items'], sep = '_')
                df_bruto.loc[:, 'facilityId'] = pd.to_numeric(df_bruto['facilityId'], errors='coerce')

                df = pd.DataFrame()
                df['Nome'] = df_bruto['name'] 
                df['facilityId'] = df_bruto['facilityId']
                df['id'] = df_bruto['gatewayId']

                df_final_1 = df.merge(df_banco, how='left', on='facilityId')

                sql_query_2 = """
                SELECT recent_activations."gatewayId" AS id,
                    TO_CHAR(recent_activations."createdAt" AT TIME ZONE 'UTC+3', 'DD/MM/YYYY HH24:MI') AS "Ativação", 
                    u.name AS "userAtivação"
                FROM (
                    SELECT ga."gatewayId",
                        ga.action,
                        ga."createdAt",
                        ga."userId",
                        ROW_NUMBER() OVER (PARTITION BY ga."gatewayId" ORDER BY ga."createdAt" DESC) AS row_num
                    FROM "tbGatewayActivatorHistory" ga
                    WHERE ga.action = 'ACTIVATE'
                ) AS recent_activations
                JOIN "tbUser" u ON u.id = recent_activations."userId"
                WHERE recent_activations.row_num = 1
                ORDER BY recent_activations."createdAt" desc
                """

                df_ativ = pd.read_sql_query(sql_query_2, engine)
                df_final = df_final_1.merge(df_ativ, how='left', on='id')

                df_final = df_final[df_final['Empresa'].notna() & (df_final['Nome'] != "")]
                return df_final

            def authenticate_cognito(username, password, client_id, region):
                client = boto3.client("cognito-idp", region_name=region)
                response = client.initiate_auth(
                    AuthFlow="USER_PASSWORD_AUTH",
                    AuthParameters={"USERNAME": username, "PASSWORD": password},
                    ClientId=client_id,
                )
                return response["AuthenticationResult"]["AccessToken"]

            def download_data(data_url, token):
                headers = {"Authorization": f"Bearer {token}"}
                response = requests.get(data_url, headers=headers)
                response.raise_for_status()
                data = response.json()
                return pd.DataFrame(data.get("data", []))

            def clean_tb_occurrence(dataframe):
                dataframe["phone*"] = dataframe["phone*"].astype(str)
                df_null_phones = dataframe[dataframe["phone*"].isin(["nan", "null"])]
                df_null_phones = df_null_phones[["name*", "mail"]]
                dataframe = dataframe[~dataframe["phone*"].isin(["nan", "null"])]
                return dataframe, df_null_phones

            token = authenticate_cognito(username, password, client_id, region)

            data = download_data(data_url, token)

            columns = ["email", "phone", "cognitoId"]

            data = data[columns]
 
            sql_query = f"""
            WITH "users" AS (
                SELECT 
                    u.id AS "cognitoId",
                    u.name AS "User",
                    u.profile AS "Profile",
                    COALESCE(
                        JSON_AGG(DISTINCT uc."companyId") FILTER (WHERE uc."companyId" IS NOT NULL),
                        COALESCE(JSON_AGG(DISTINCT c.id) FILTER (WHERE c.id IS NOT NULL), '[]')
                    ) AS "Companies",
                    COALESCE(
                        JSON_AGG(DISTINCT uf."facilityId") FILTER (WHERE uf."facilityId" IS NOT NULL),
                        COALESCE(JSON_AGG(DISTINCT ucf.id) FILTER (WHERE ucf.id IS NOT NULL), '[]')
                    ) AS "Facilities"
                FROM 
                    "tbUser" u
                LEFT JOIN "tbCompany" c ON c.id = u."companyId"
                LEFT JOIN "tbUserFacility" uf ON uf."userId" = u.id
                LEFT JOIN "tbFacility" uff ON uff.id = uf."facilityId"
                LEFT JOIN "tbCompany" ufc ON ufc.id = uff."companyId"
                LEFT JOIN "tbUserCompany" uc ON uc."userId" = u.id
                LEFT JOIN "tbCompany" ucc ON ucc.id = uc."companyId"
                LEFT JOIN "tbFacility" ucf ON ucf."companyId" = uc."companyId"
                WHERE 
                    u."deletedAt" IS NULL
                    AND u.profile NOT IN ('MESA_ANALISTA', 'MESA_MASTER', 'REPRESENTANTE', 'EXECUTIVO_MASTER')
                GROUP BY 
                    u.id, u.name, u.profile
            )
            SELECT DISTINCT ON (u."cognitoId") 
                u.*
            FROM "users" u
            ORDER BY u."cognitoId";
            """
            df_query = pd.read_sql_query(sql_query, engine)
            df_query = df_query.applymap(clean_string)

            df_query['cognitoId'] = df_query['cognitoId'].fillna("").astype(str).str.strip().str.lower()
            data['cognitoId'] = data['cognitoId'].fillna("").astype(str).str.strip().str.lower()

            data = df_query.merge(data,on="cognitoId", how='left')
            
            template_map = {
                "Ocorrência": 1,
                "Mudança de Comportamento": 2,
                "Temperatura Elevada": 3
            }
            template_input = template_map.get(template, 4)

            if template_input in [1, 3]:
                template = (
                    "ocorrencia" if template_input == 1 
                    else "temperatura_elevada" 
                )
                img = (
                    "Confidential" if template_input == 1 
                    else "Confidential" 
                )

                sql_query = f"""
                SELECT
                    ao.id AS "Id",
                    c.name AS "Empresa", 
                    c.id AS "companyId",
                    f.name AS "Unidade",
                    f.id AS "facilityId",
                    a.name AS "Equipamento",
                    a.id AS "EquipamentoId"
                FROM "tbAssetOccurrence" ao
                JOIN "tbCompany" c ON c.id = ao."tenantId" AND c."deletedAt" IS NULL AND c.name <> 'IBBx '
                JOIN "tbAsset" a ON a.id = ao."assetId" AND a."deletedAt" IS NULL
                JOIN "tbFacility" f ON f.id = a."facilityId" AND f."deletedAt" IS NULL      
                WHERE ao."createdAt" > '2024-01-01'
                AND c."deletedAt" IS NULL
                AND ao.id = {user_input}
                """

                query_ocurrence = pd.read_sql_query(sql_query, engine)

                query_ocurrence = query_ocurrence.applymap(clean_string)

                for _, row in query_ocurrence.iterrows():
                    id_ocorrencia = row["Id"]
                    empresa = row["Empresa"]
                    companyId = row["companyId"]
                    unidade = row["Unidade"]
                    facilityId = row["facilityId"]
                    var1 = row["Equipamento"]
                    var2 = row["EquipamentoId"]

                filtered_data_company = data.loc[
                    (data["Companies"].apply(lambda x: companyId in x)) & 
                    ((data["Facilities"].apply(lambda x: facilityId in x)) | (data["Facilities"] == "[]"))
                ]

                botão = f"Confidential"            

            if template_input == 2:
                template = "mud_de_comportamento_1"
                img = "Confidential"

                sql_query = f"""
                    SELECT c.name as "Empresa",
                    c.id as "companyId",
                    f.name as "Unidade",
                    f.id as "facilityId",
                    a.name as "Equipamento",
                    a.id as "EquipamentoId"
                    FROM "tbAsset" a 
                    JOIN "tbFacility" f ON f.id = a."facilityId"
                    JOIN "tbCompany" c ON c.id = f."tenantId"
                    WHERE a.id = {user_input}
                    """
                query_ocurrence = pd.read_sql_query(sql_query, engine)

                query_ocurrence = query_ocurrence.applymap(clean_string)

                for _, row in query_ocurrence.iterrows():
                    empresa = row["Empresa"]
                    companyId = row["companyId"]
                    unidade = row["Unidade"]
                    facilityId = row["facilityId"]
                    var1 = row["Equipamento"]
                    var2 = row["EquipamentoId"]

                filtered_data_company = data.loc[
                    (data["Companies"].apply(lambda x: companyId in x)) & 
                    ((data["Facilities"].apply(lambda x: facilityId in x)) | (data["Facilities"] == "[]"))
                ]

                botão = f"Confidential"
                    
            if template_input == 4:
                template = "conectividade"
                img = "Confidential"
                data_bolt = script_bolt()

                data_bolt = data_bolt.applymap(clean_string)

                data_bolt = data_bolt.loc[data_bolt["id"] == user_input]

                for _, row in data_bolt.iterrows():
                    empresa = row["Empresa"]
                    companyId = row["companyId"]
                    unidade = row["Unidade"]
                    facilityId = row["facilityId"]
                    var1 = row["Nome"]

                filtered_data_company = data.loc[
                    (data["Companies"].apply(lambda x: companyId in x)) & 
                    ((data["Facilities"].apply(lambda x: facilityId in x)) | (data["Facilities"] == "[]"))
                ]

                botão = f"Confidential"

            dataframe = pd.DataFrame({
                "phone*": filtered_data_company["phone"],
                "name*": filtered_data_company["User"],
                "mail": filtered_data_company["email"],
                "template*": template,
                "lang*": "pt_BR",
                "id_whatsapp*": id_whatsapp,
                "tag": None,
                "titulo_variavel_1_URL_IMAGE*": img,
                "descri_variavel_1*": var1.title(),
                "botao_variavel_1*": botão
            })

            if 'phone*' in dataframe.columns:
                dataframe['phone*'] = dataframe['phone*'].str.replace('+', '', regex=False)
                dataframe['phone*'] = dataframe['phone*'].apply(lambda x: x[:2] + ' ' + x[2:] if isinstance(x, str) and len(x) > 2 else x)

            dataframe["phone*"] = dataframe["phone*"].apply(lambda x: "null" if len(str(x)) < 8 else x)
            dataframe["name*"] = dataframe["name*"].apply(clean_and_title)
            dataframe, df_null_phones = clean_tb_occurrence(dataframe)

            link = "https://Confidential.com" + botão

            output_file = f"{template}.csv"
            dataframe.to_csv(output_file, index=False, sep=";", encoding='utf-8-sig')

            df_null_phones.to_excel(f"Null Phones {empresa}.xlsx", index=False)
            self.show_success_message(empresa, unidade, link)

    def show_success_message(self, empresa, unidade, link):
        if self.message_label:
            self.message_label.destroy()

        self.message_label = ctk.CTkFrame(self.inputs_frame)
        self.message_label.pack(pady=10, fill="x", padx=10)

        empresa_label = ctk.CTkLabel(
            self.message_label,
            text=f"\nEmpresa:",
            font=("Arial", 14, "bold"),
        )
        empresa_label.pack(pady=5, anchor="center")

        empresa_label_var = ctk.CTkLabel(
            self.message_label,
            text=f"\n{empresa}",
            font=("Arial", 14),
        )
        empresa_label_var.pack(pady=5, anchor="center")

        unidade_label = ctk.CTkLabel(
            self.message_label,
            text=f"\nUnidade:",
            font=("Arial", 14, "bold"),
            justify="center"
        )
        unidade_label.pack(pady=10, anchor="center")

        unidade_label_var = ctk.CTkLabel(
            self.message_label,
            text=f"{unidade}",
            font=("Arial", 14),
            justify="center"
        )
        unidade_label_var.pack(pady=10, anchor="center")

        success_label = ctk.CTkLabel(
            self.message_label,
            text="\nTemplate gerado com sucesso!\n",
            font=("Arial", 14, "bold"),
            text_color="green"
        )
        success_label.pack(pady=10, anchor="center")

        button_frame = ctk.CTkFrame(self)
        button_frame.pack(side="bottom", pady=10)

        link_button = ctk.CTkButton(
            button_frame,
            text="No Retina",
            command=lambda: webbrowser.open(link),
            width=100
        )
        link_button.pack(side="left", padx=2)

        def open_directory():
            directory_path = os.getcwd()
            os.startfile(directory_path)

        view_files_button = ctk.CTkButton(
            button_frame,
            text="Ver Arquivos",
            command=open_directory,
            width=100
        )
        view_files_button.pack(side="left", padx=2)

        view_link = ctk.CTkButton(
            button_frame,
            text="Abrir Zendesk",
            command=lambda: webbrowser.open("Confidential"),
            width=100
        )
        view_link.pack(side="left", padx=2)

if __name__ == "__main__":
    ctk.set_appearance_mode("Dark")  
    ctk.set_default_color_theme("blue")  
    app = TemplateSelectorApp()
    app.mainloop()
