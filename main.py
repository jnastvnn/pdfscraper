import json
import os
import re
import sqlite3
import PyPDF2
import requests
import pandas

db = sqlite3.connect('imported_companies.db')
dfs = pandas.read_excel('fileWithCompanies.xlsx', sheet_name=None)
for table, df in dfs.items():
    df.to_sql(table, db)
    print(f'{df} inserted successfully')

KEYWORDS = ["keyword1", "keyword2", "keyword3", "keyword4"]
conn = sqlite3.connect('imported_companies.db')
c = conn.cursor()
c2 = conn.cursor()


# taulukko pitää alustaa vain ensimmäisellä kerralla
# c.execute("""DROP TABLE companies""")
# c.execute("""CREATE TABLE companies (
#                                         company_num text,
#                                         name_from_file text,
#                                         url_1 text DEFAULT '',
#                                         url_2 text DEFAULT '',
#                                         url_3 text DEFAULT '',
#                                         UNIQUE(company_num)
#                                     )""")

# c.execute("""DROP TABLE pdf_results""")
# c.execute("""CREATE TABLE pdf_results (
#                                         company_num integer,
#                                         pdf_1_location text DEFAULT 'No file location',
#                                         pdf_2_location text DEFAULT 'No file location',
#                                         pdf_3_location text DEFAULT 'No file location',
#                                         UNIQUE(company_num)
#                                     )""")

def urlSearch(query, use_case='search'):
    url = f"https://google.serper.dev/{use_case}"

    payload = json.dumps({
        "q": f"{query}",
        "gl": "fi",
        "hl": "fi",
        "autocorrect": False
    })
    headers = {
        'X-API-KEY': '------------------------------------',
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload).json()
    out = []
    for i in range(3):
        try:
            out.append(response['organic'][i]['link'])
        except IndexError:
            out.append('')
    print(out)
    return out


def insert_company(co_id):
    with conn:
        c.execute("INSERT OR IGNORE INTO pdf_results (company_num) VALUES (?)", ([int(co_id)]))


def decrypt(filePath):
    os.system(f"qpdf --password= --decrypt --replace-input \"{filePath}\"")
    print('File Decrypted (qpdf)')


def check_pdf_encryption(location):
    try:
        filepath = os.path.join(location)
        f = open(filepath, 'rb')

        try:
            pdf = PyPDF2.PdfReader(f)
            if pdf.is_encrypted:

                try:
                    pdf.decrypt('')
                    print('File Decrypted (PyPDF2)')
                    return True
                except:
                    f.close()
                    decrypt(filepath)
                    check_pdf_encryption(location)

            else:
                print('File Not Encrypted')
            with open(filepath, "rb") as fp:
                pdf = PyPDF2.PdfReader(fp)
                info = pdf.metadata
                if info:
                    return True
                else:
                    print(info)
                    return False
        except:
            return False

    except FileNotFoundError:
        return False


def download_pdf(url, co_num, pdf_num):
    pdf = ''
    filename = url.split("/")[-1].rsplit("?", 1)[0]
    try:

        header = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, '
                          'like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

        r = requests.get(url, timeout=(10, 10), stream=True, headers=header)

        if "Content-Disposition" in r.headers.keys():
            if len(re.findall("filename=(.+)", r.headers["Content-Disposition"])) == 0:
                filename = url.split("/")[-1].rsplit("?", 1)[0]

        else:
            filename = url.split("/")[-1].rsplit("?", 1)[0]

        pdf = f"{'pdf_files/'}{filename}"
        print(f"Downloading pdf: ", url, "\n")
        try:
            if filename == '':
                return False
            with open(pdf, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=2000):
                    fd.write(chunk)
        except FileNotFoundError or OSError:
            print(f"File {filename}.pdf not found")
    except requests.exceptions.RequestException:
        print("Connection error", url)
    if check_pdf_encryption(pdf):
        with conn:
            c.execute(f"""UPDATE pdf_results SET \"pdf_{pdf_num + 1}_location\" = (?) WHERE company_num = (?)""",
                      ([pdf, co_num]))


def updateDownloadLink(select, fromTable, where, newValue, co_id):
    with conn:
        c.execute(f"""UPDATE \"{fromTable}\" SET \"{select}\" = (?) 
        WHERE \"{where}\" = (?)""", ([newValue, co_id]))


def readPdf(filename, pdf_num, co_id):
    index = 0
    print("Searching key words from: ", filename)
    location = filename
    try:
        obj = PyPDF2.PdfReader(location)
        NumPages = len(obj.pages)

        # extract text and do the search
        for pagenum in range(0, NumPages):
            PageObj = obj.pages[pagenum]

            Text = PageObj.extract_text().lower()

            for word in KEYWORDS:

                if word in Text:
                    print(f"                     {word} found on page {str(pagenum + 1)}")
                    with conn:
                        c.execute(
                            f"""SELECT \"{word}_results_from_pdf_{pdf_num}\" FROM pdf_results WHERE company_num = \"{co_id}\"""")

                        previous_results = str(c.fetchone()[0])
                        print(previous_results)
                        if str(pagenum + 1) not in previous_results:
                            if previous_results is None:
                                previous_results = str(pagenum + 1)
                            else:
                                previous_results += ", " + str(pagenum + 1)

                        c.execute(f"""UPDATE pdf_results SET \"{word}_results_from_pdf_{pdf_num}\" = (?) 
                        WHERE company_num = (?)""", ([previous_results, co_id]))


    except OverflowError:
        index += 1


def get_download_link(item_id, link_num):
    c2.execute(f"""SELECT \"url_{link_num}\" FROM Sheet1 WHERE UlrikeID = \"{item_id}\"""")
    link = str(c2.fetchone()[0])
    return link


def main():
    # tuodaan json tiedosto ja lisätään tietokantaan companies.db
    for i in range(3):
        try:
            c.execute(f"""ALTER TABLE Sheet1 ADD COLUMN \"url_{i}\" text """)
        except sqlite3.OperationalError:
            continue

    for keyw in KEYWORDS:
        for i in range(3):
            try:
                c.execute(f"""ALTER TABLE pdf_results ADD COLUMN \"{keyw}_results_from_pdf_{i}\" text """)
            except sqlite3.OperationalError:
                continue

    # c.execute("""SELECT UlrikeID FROM Sheet1""")
    # for co_id in c.fetchall():
    #     insert_company(co_id[0])

    # Hakee hakusanalla url:t
    c.execute("""SELECT UlrikeID, Riskinkohde  FROM Sheet1""")
    for row in c.fetchall():
        co_id, co_name = row
        url_0, url_1, url_2 = urlSearch(f'{co_name} hakusana')
        updateDownloadLink('url_0', 'Sheet1', 'UlrikeID', url_0, co_id)
        updateDownloadLink('url_1', 'Sheet1', 'UlrikeID', url_1, co_id)
        updateDownloadLink('url_2', 'Sheet1', 'UlrikeID', url_2, co_id)

    c.execute("""SELECT pdf_1_location, pdf_2_location, pdf_3_location, company_num  FROM pdf_results""")
    for row in c.fetchall():
        co_num = row[3]

        for i in range(3):
            if row[i] == 'No file location' and co_num > 17000:
                link = get_download_link(co_num, i)
                if len(link) == 0:
                    continue
                # tarkistaa onko linkki ladattava pdf tiedosto
                try:

                    r = requests.get(link, timeout=(10, 10))
                except requests.exceptions.SSLError:
                    continue
                except requests.exceptions.ReadTimeout:
                    continue
                except requests.exceptions.ConnectTimeout:
                    continue
                except requests.exceptions.ConnectionError:
                    continue

                content_type = r.headers.get('content-type')
                try:
                    if 'application/pdf' in content_type:
                        print(f'company id: {co_num}')
                        download_pdf(link, co_num, i)
                except TypeError:
                    continue

    with conn:
        c.execute("""SELECT pdf_1_location, pdf_2_location, pdf_3_location, company_num FROM pdf_results""")
        data = c.fetchall()
    for row in data:
        for i in range(3):
            co_num = row[3]
            if row[i] == 'No file location':
                continue
            readPdf(row[i], i, co_num)

    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
