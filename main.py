import calendar
import json
import os
import re
import sqlite3

import PyPDF2
import requests

KEYWORDS = ["scope 1", "scope 2", "scope 3", "co2e"]
conn = sqlite3.connect('companies.db')
c = conn.cursor()
c2 = conn.cursor()


# taulukko pitää alustaa vain ensimmäisellä kerralla
#c.execute("""DROP TABLE companies""")
# c.execute("""CREATE TABLE companies (
#                                         company_num text,
#                                         name_from_file text,
#                                         name_from_url_search text,
#                                         category text,
#                                         urlToSearch text,
#                                         url_1 text,
#                                         url_2 text,
#                                         url_3 text,
#                                         UNIQUE(company_num)
#                                     )""")

#c.execute("""DROP TABLE pdf_results""")
# c.execute("""CREATE TABLE pdf_results (
#                                         company_num text,
#                                         pdf_1_location text DEFAULT 'No file location',
#                                         pdf_2_location text DEFAULT 'No file location',
#                                         pdf_3_location text DEFAULT 'No file location',
#                                         UNIQUE(company_num)
#                                     )""")


def insert_company(co):
    with conn:
        num = str(co['company num.']).replace(',','')
        c.execute(
            "INSERT OR IGNORE INTO companies VALUES (:company_num, :name_from_file, :name_from_url_search, "
            ":category, :urlToSearch, :url_1, :url_2, :url_3)",
            {
                'company_num': num,
                'name_from_file': co['name from file'],
                'name_from_url_search': co['name from url search'],
                'category': co['category'],
                'urlToSearch': co['urlToSearch'],
                'url_1': co['keywords']['first result'],
                'url_2': co['keywords']['second result'],
                'url_3': co['keywords']['third result']
            })
        c.execute("INSERT OR IGNORE INTO pdf_results (company_num) VALUES (?)", ([num]))


# toistaiseksi turha funktio

# def read_companies():
#     with open(file="test.txt", mode="r") as file:
#         for line in file:
#             name, co_id = line.rstrip().split(";")


def decrypt(fullfile):
    os.system(f"qpdf --password= --decrypt --replace-input \"{fullfile}\"")
    print('File Decrypted (qpdf)')


def check_pdf_encryption(location):
    try:
        fullfile = os.path.join(location)
        f = open(fullfile, 'rb')

        try:

            pdf = PyPDF2.PdfFileReader(f)
            if pdf.isEncrypted:

                try:
                    pdf.decrypt('')
                    print('File Decrypted (PyPDF2)')
                    return True
                except:
                    f.close()
                    decrypt(fullfile)
                    check_pdf_encryption(location)

            else:
                print('File Not Encrypted')
            with open(fullfile, "rb") as fp:
                pdf = PyPDF2.PdfFileReader(fp)
                info = pdf.getDocumentInfo()
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

        r = requests.get(url, timeout=200, stream=True, headers=header)

        if "Content-Disposition" in r.headers.keys():
            if len(re.findall("filename=(.+)", r.headers["Content-Disposition"])) == 0:
                filename = url.split("/")[-1].rsplit("?", 1)[0]

        else:
            filename = url.split("/")[-1].rsplit("?", 1)[0]

        pdf = f"{'pdf_files/'}{filename}"
        print(f"Downloading: ", url, "\n")
        try:
            with open(pdf, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=2000):
                    fd.write(chunk)
        except FileNotFoundError or OSError:
            print(f"File {filename}.pdf not found")
    except requests.exceptions.RequestException:
        print("Connection error", url)
    if check_pdf_encryption(pdf):
        last_pdf_index = pdf.rfind(".pdf")

        # Check if there is more than one ".pdf" in the string
        if last_pdf_index != len(pdf) - 4:
            # If there is, remove the excess ".pdf" by slicing the string
            pdf = pdf[:last_pdf_index + 4]
        with conn:
            c.execute(f"""UPDATE pdf_results SET \"pdf_{pdf_num + 1}_location\" = (?) WHERE company_num = (?)""",
                      ([pdf, co_num]))


def readPdf(filename, pdf_num, id):
    index = 0
    print("Searching key words from: ", filename)
    location = filename
    try:
        obj = PyPDF2.PdfFileReader(location)
        NumPages = obj.getNumPages()

        # extract text and do the search
        for i in range(0, NumPages):
            PageObj = obj.getPage(i)

            Text = PageObj.extractText().lower()


            for word in KEYWORDS:

                if word in Text:
                    print(f"                     {word} found on page {str(i + 1)}")
                    with conn:
                        c.execute(f"""SELECT \"{word}_results_from_pdf_{pdf_num + 1}\" FROM pdf_results WHERE company_num = \"{id}\"""")
                        previous_results = str(c.fetchone()[0]) + ", " + str(i + 1)
                        c.execute(f"""UPDATE pdf_results SET \"{word}_results_from_pdf_{pdf_num +1}\" = (?) 
                        WHERE company_num = (?)""", ([previous_results, id]))


    except OverflowError:
        index += 1


def get_download_link(id, link_num):
    c2.execute(f"""SELECT \"url_{link_num + 1}\" FROM companies WHERE company_num = \"{id}\"""")
    link = str(c2.fetchone()[0])
    return link


def main():
    # tuodaan json tiedosto ja lisätään tietokantaan companies.db
    with open('dataFVB.json') as json_file:
        data = json.load(json_file)
    for ind in data:
        insert_company(data[ind][0])

    for keyw in KEYWORDS:
        for i in range(3):
            try:
                c.execute(f"""ALTER TABLE pdf_results ADD COLUMN \"{keyw}_results_from_pdf_{i}\" text """)
            except sqlite3.OperationalError:
                continue

    # c.execute("""SELECT url_1, url_2, url_3, company_num  FROM companies""")
    c.execute("""SELECT pdf_1_location, pdf_2_location, pdf_3_location, company_num  FROM pdf_results""")
    for row in c.fetchall():
        co_num = row[3]

        for i in range(3):
            if row[i] == 'No file location':
                link = get_download_link(co_num, i + 1)
                # tarkistaa onko linkki ladattava pdf tiedosto
                if str(link.find('.pdf')) != -1 and len(str(link.find('.pdf'))) > 2:
                    download_pdf(link, co_num, i)
    with conn:
        c.execute("""SELECT pdf_1_location, pdf_2_location, pdf_3_location, company_num FROM pdf_results""")
        data = c.fetchall()
    for row in data:
        for i in range(3):
            co_num = row[3]
            if row[i] == 'No file location':
                continue
            readPdf(row[i], i, co_num)

    # c.execute("SELECT * FROM companies WHERE company_num=104")
    # print(c.fetchone())

    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
