# %%
import pandas as pd
from sqlalchemy import create_engine

# %%
class PortfolioProject():
    def __init__(self) -> None:
        # PostgreSQL connection
        self.user = 'postgres'
        self.password = 'Greciagreciagrecia3'
        self.host = 'postgres'
        self.port = '5432'
        self.db = 'PortfolioProject'
        self.path = f'/Users/mariajosefranco/Documents/coding/Data Analytics Project/owid-covid-data.csv'

    def obtaining_tables(self):
        df = pd.read_csv(self.path)
        # For simplification of the joins
        population_col = df.pop('population')
        df.insert(4, 'population', population_col)

        # Dataframe of Covid Deaths
        idx = df.columns.get_loc('total_tests')
        df_covid_deaths = df.iloc[:, 0:idx]

        # Dataframe of Covid Vaccinations
        df_aux1 = df.iloc[:, 0:4]
        df_aux2 = df.iloc[:, idx:]
        df_covid_vaccinations = pd.concat([df_aux1, df_aux2], axis=1)
        return df_covid_deaths, df_covid_vaccinations

    def table_to_csv(self, df, df_name):
        df.to_csv(df_name + '.csv', ',', index=False)

    def connecting_sql(self):
        engine = create_engine(f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}/{self.db}')
        try:
            connection = engine.connect()
            print("Conexión exitosa a PostgreSQL!")

            # Aquí puedes realizar operaciones en la base de datos

            # Cerrar la conexión
            connection.close()
            print("Conexión cerrada.")

        except Exception as e:
            print("Error de conexión:", e)

    def main(self):
        df_covid_deaths, df_covid_vaccinations = self.obtaining_tables()
        self.table_to_csv(df_covid_deaths, 'covid_deaths')
        self.table_to_csv(df_covid_vaccinations, 'covid_vaccinations')
        #self.connecting_sql()
        return df_covid_deaths, df_covid_vaccinations


# %%
if __name__ == '__main__':
    PP = PortfolioProject()
    df_covid_deaths, df_covid_vaccinations = PP.main()



# %%
