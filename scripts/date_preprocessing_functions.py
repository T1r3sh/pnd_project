import re
import json
import pandas as pd


def table_data_unpack(table_data_string: str) -> list[dict]:
    output = []
    pattern = r"{[^{}]*}"
    result = re.findall(pattern, table_data_string)
    for res in result:
        test_fixed = res.replace('"', "")
        test_fixed = test_fixed.replace("'", '"')
        tmp_dict = json.loads(test_fixed)
        if tmp_dict:
            output.append(tmp_dict)
    return output


if __name__ == "__main__":
    print("ipip")

    broken_setup = "[{}, {}, {}, {}, {'№ п/п': '209', 'Эмитент (Управляющая компания)': \"BNY Mellon (O'KEY Group S.A. (АО О'КЕЙ ГРУПП)\", 'Тип,\\nвид': 'ДР', 'Государственный\\nрегистрационный\\nномер\\n(Номер правил доверительного управления)': '\\xa0', 'ISIN': 'US6708662019', 'Код ценной\\nбумаги': 'OKEY', 'Проведение торгов': 'ü', 'Особенности': 'ü', '\"Режим основных торгов Т+\"/ \"Сектор ПИР – Режим основных торгов\" ': 'ü', '\"РПС с ЦК\"/ \"Сектор ПИР – РПС с ЦК\"': 'ü', '\"РЕПО с ЦК – Адресные заявки\"': 'ü', '\"РЕПО с ЦК – Безадресные заявки\"': 'ü', 'Режим переговорных сделок (РПС)/ \"Сектор ПИР – РПС\"': '\\xa0', '\"Междилерское РЕПО\"': '\\xa0', '\\nДля квалифицированных инвесторов': '\\xa0', '\\nСектор ПИР': '\\xa0'}]"
    print(table_data_unpack(broken_setup))

    df = pd.read_csv("moex_value_deviation_dataset.csv")
    df.dropna(inplace=True)
    df["explicit_table_data"] = df["table_data"].apply(lambda x: table_data_unpack(x))
