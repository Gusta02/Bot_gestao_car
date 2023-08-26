import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Configuração do SQLAlchemy
SQLALCHEMY_DATABASE_URL = 'postgresql://postgres:140546420@localhost:5432/Gestao_car'
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Declaração da base para a criação da tabela no banco de dados
Base = declarative_base()

# Classe para mapear a tabela no banco de dados
class Carros(Base):
    __tablename__ = "Top_Carros_SP"

    id = Column(Integer, primary_key=True, index=True)
    Modelo = Column(String)
    Ano = Column(String)
    Kilometragem = Column(String)
    Preco = Column(String)
    Link = Column(String)
    Data_Extracao = Column(DateTime)
    Fabricante = Column(String)

# Lista de modelos e fabricantes
carros = [
    {"modelo": "polo", "fabricante": "Volkswagen"}
    # ,{"modelo": "t-cross", "fabricante": "Volkswagen"},
    # {"modelo": "nivus", "fabricante": "Volkswagen"},
    # {"modelo": "logan", "fabricante": "Renault"},
    # {"modelo": "kwid", "fabricante": "Renault"},
    # {"modelo": "virtus", "fabricante": "Volkswagen"},
    # {"modelo": "hb20", "fabricante": "Hyundai"},
    # {"modelo": "kicks", "fabricante": "Nissan"},
    # {"modelo": "hr-v", "fabricante": "Honda"},
    # {"modelo": "renegade", "fabricante": "Jeep"},
    # {"modelo": "hb20s", "fabricante": "Hyundai"},
    # {"modelo": "onix", "fabricante": "Chevrolet"},
    # {"modelo": "creta", "fabricante": "Hyundai"}
]

for carro in carros:
    url = f"https://lista.mercadolivre.com.br/veiculos/carros-caminhonetes/{carro['fabricante']}/{carro['modelo']}-em-sao-paulo/carro_NoIndex_True#applied_filter_id%3DMODEL%26applied_filter_name%3DModelo%26applied_filter_order%3D4%26applied_value_id%3D62109%26applied_value_name%3D{carro['modelo']}%26applied_value_order%3D8%26applied_value_results%3D1570%26is_custom%3Dfalse"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        
        ol_elements = soup.find_all("ol", class_="ui-search-layout ui-search-layout--grid")
        car_names = []
        car_years = []
        car_kilometers = []
        car_prices = []
        car_links = []
        
        for ol_element in ol_elements:
            car_elements = ol_element.find_all("li", class_="ui-search-layout__item")

            for car_element in car_elements:
                name_element = car_element.find("h2", class_="ui-search-item__title shops__item-title")
                car_names.append(name_element.text.strip() if name_element else "Nome não disponível")

                attribute_elements = car_element.find_all("li", class_="ui-search-card-attributes__attribute")

                if len(attribute_elements) >= 1:
                    car_years.append(attribute_elements[0].text.strip())
                else:
                    car_years.append("Ano não disponível")

                if len(attribute_elements) >= 2:
                    car_kilometers.append(attribute_elements[1].text.strip())
                else:
                    car_kilometers.append("Kilometragem não disponível")

                price_element = car_element.find("span", class_="andes-money-amount ui-search-price__part shops__price-part ui-search-price__part--medium andes-money-amount--cents-superscript")
                car_prices.append(price_element.text.strip() if price_element else "Preço não disponível")

                link_element = car_element.find("a", class_="ui-search-item__group__element shops__items-group-details ui-search-link")
                car_links.append(link_element["href"] if link_element else "Link não disponível")

        current_date = datetime.now()
        
        data = {
            "Modelo": car_names,
            "Ano": car_years,
            "Kilometragem": car_kilometers,
            "Preco": car_prices,
            "Link": car_links,
            "Data_Extracao": current_date,
            "Fabricante": carro["fabricante"]
        }
        
        df = pd.DataFrame(data)
        
        # Criar uma lista de objetos Carros
        car_objects = [
            Carros(
                Modelo=row["Modelo"],
                Ano=row["Ano"],
                Kilometragem=row["Kilometragem"],
                Preco=row["Preco"],
                Link=row["Link"],
                Data_Extracao=row["Data_Extracao"],
                Fabricante=row["Fabricante"]
            ) for _, row in df.iterrows()
        ]
        
        # Inserir os dados no banco de dados
        session = SessionLocal()
        session.add_all(car_objects)
        session.commit()
        session.close()

        print("no banco jão")
    else:
        print("Falha ao obter a página:", response.status_code)