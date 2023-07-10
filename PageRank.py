# This file is part of PageRank application developed by Stefan-Mihai MOGA.
#
# PageRank is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the Open
# Source Initiative, either version 3 of the License, or any later version.
#
# PageRank is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# PageRank.  If not, see <http://www.opensource.org/licenses/gpl-3.0.html>

import mysql.connector
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from urllib.parse import urljoin
import urllib
import time
from gensim.utils import tokenize
import networkx as nx


HOSTNAME = "localhost"
DATABASE = "r46882text_mysql_test"
USERNAME = "r46882text_engine"
PASSWORD = "TextMining2021!@#$"

webpage_count = -1


def create_database():
    try:
        connection = mysql.connector.connect(
            host=HOSTNAME,
            database=DATABASE,
            user=USERNAME,
            password=PASSWORD,
            autocommit=True,
        )
        server_info = connection.get_server_info()
        print("MySQL connection is open on", server_info)
        sql_drop_table = "DROP TABLE IF EXISTS `occurrence`"
        cursor = connection.cursor()
        cursor.execute(sql_drop_table)
        sql_drop_table = "DROP TABLE IF EXISTS `hyperlink`"
        cursor.execute(sql_drop_table)
        sql_drop_table = "DROP TABLE IF EXISTS `keyword`"
        cursor.execute(sql_drop_table)
        sql_drop_table = "DROP TABLE IF EXISTS `webpage`"
        cursor.execute(sql_drop_table)
        sql_drop_table = "DROP TABLE IF EXISTS `url`"
        cursor.execute(sql_drop_table)
        sql_create_table = (
            "CREATE TABLE `url` (`url_id` BIGINT NOT NULL AUTO_INCREMENT, "
            "`address` VARCHAR(256) NOT NULL, `visited` BOOL NOT NULL, "
            "`score` BIGINT NOT NULL, `pagerank` DOUBLE NOT NULL, "
            "PRIMARY KEY(`url_id`)) ENGINE=InnoDB"
        )
        cursor.execute(sql_create_table)
        sql_create_index = "CREATE UNIQUE INDEX url_address ON `url`(`address`)"
        cursor.execute(sql_create_index)
        sql_create_table = (
            "CREATE TABLE `webpage` (`webpage_id` BIGINT NOT NULL AUTO_INCREMENT, "
            "`url_id` BIGINT NOT NULL, `title` VARCHAR(256) NOT NULL, "
            "`content` TEXT NOT NULL, PRIMARY KEY(`webpage_id`),"
            "FOREIGN KEY webpage_fk(url_id) REFERENCES url(url_id)) ENGINE=InnoDB"
        )
        cursor.execute(sql_create_table)
        sql_create_table = (
            "CREATE TABLE `keyword` (`keyword_id` BIGINT NOT NULL AUTO_INCREMENT, "
            "`name` VARCHAR(256) NOT NULL, PRIMARY KEY(`keyword_id`)) ENGINE=InnoDB"
        )
        cursor.execute(sql_create_table)
        sql_create_index = "CREATE UNIQUE INDEX keyword_name ON `keyword`(`name`)"
        cursor.execute(sql_create_index)
        sql_create_table = (
            "CREATE TABLE `hyperlink` (`from_page` BIGINT NOT NULL, "
            "`to_page` BIGINT NOT NULL, `counter` BIGINT NOT NULL, "
            "PRIMARY KEY(`from_page`, `to_page`), "
            "FOREIGN KEY from_page_fk(from_page) REFERENCES url(url_id), "
            "FOREIGN KEY to_page_fk(to_page) REFERENCES url(url_id)) ENGINE=InnoDB"
        )
        cursor.execute(sql_create_table)
        sql_create_table = (
            "CREATE TABLE `occurrence` (`webpage_id` BIGINT NOT NULL, "
            "`keyword_id` BIGINT NOT NULL, `counter` BIGINT NOT NULL, "
            "PRIMARY KEY(`webpage_id`, `keyword_id`), "
            "FOREIGN KEY webpage_fk(webpage_id) REFERENCES webpage(webpage_id), "
            "FOREIGN KEY keyword_fk(keyword_id) REFERENCES keyword(keyword_id)) ENGINE=InnoDB"
        )
        cursor.execute(sql_create_table)
    except mysql.connector.Error as err:
        print("MySQL connector error:", str(err))
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is now closed")
        return True


def add_url_to_frontier(connection, url):
    if url.find("#") > 0:
        url = url.split("#")[0]
    if len(url) > 256:
        return False
    if url.endswith(".3g2"):
        return False  # 3GPP2 multimedia file
    if url.endswith(".3gp"):
        return False  # 3GPP multimedia file
    if url.endswith(".7z"):
        return False  # 7-Zip compressed file
    if url.endswith(".ai"):
        return False  # Adobe Illustrator file
    if url.endswith(".apk"):
        return False  # Android package file
    if url.endswith(".arj"):
        return False  # ARJ compressed file
    if url.endswith(".aif"):
        return False  # AIF audio file
    if url.endswith(".avi"):
        return False  # AVI file
    if url.endswith(".bat"):
        return False  # Batch file
    if url.endswith(".bin"):
        return False  # Binary disc image
    if url.endswith(".bmp"):
        return False  # Bitmap image
    if url.endswith(".cda"):
        return False  # CD audio track file
    if url.endswith(".com"):
        return False  # MS-DOS command file
    if url.endswith(".csv"):
        return False  # Comma separated value file
    if url.endswith(".dat"):
        return False  # Binary Data file
    if url.endswith(".db") or url.endswith(".dbf"):
        return False  # Database file
    if url.endswith(".deb"):
        return False  # Debian software package file
    if url.endswith(".dmg"):
        return False  # macOS X disk image
    if url.endswith(".doc") or url.endswith(".docx"):
        return False  # Microsoft Word Open XML document file
    if url.endswith(".email") or url.endswith(".eml"):
        return False  # E-mail message file from multiple e-mail clients
    if url.endswith(".emlx"):
        return False  # Apple Mail e-mail file
    if url.endswith(".exe"):
        return False  # MS-DOS executable file
    if url.endswith(".flv"):
        return False  # Adobe Flash file
    if url.endswith(".fon"):
        return False  # Generic font file
    if url.endswith(".fnt"):
        return False  # Windows font file
    if url.endswith(".gadget"):
        return False  # Windows gadget
    if url.endswith(".gif"):
        return False  # GIF image
    if url.endswith(".h264"):
        return False  # H.264 video file
    if url.endswith(".ico"):
        return False  # Icon file
    if url.endswith(".iso"):
        return False  # ISO disc image
    if url.endswith(".jar"):
        return False  # Java archive file
    if url.endswith(".jpg") or url.endswith(".jpeg"):
        return False  # JPEG image
    if url.endswith(".log"):
        return False  # Log file
    if url.endswith(".m4v"):
        return False  # Apple MP4 video file
    if url.endswith(".mdb"):
        return False  # Microsoft Access database file
    if url.endswith(".mid") or url.endswith(".midi"):
        return False  # MIDI audio file
    if url.endswith(".mov"):
        return False  # Apple QuickTime movie file
    if url.endswith(".mp3") or url.endswith(".mpa"):
        return False  # MP3 audio file
    if url.endswith(".mp4"):
        return False  # MPEG4 video file
    if url.endswith(".mpa"):
        return False  # MPEG-2 audio file
    if url.endswith(".mpg") or url.endswith(".mpeg"):
        return False  # MPEG video file
    if url.endswith(".msg"):
        return False  # Microsoft Outlook e-mail message file
    if url.endswith(".msi"):
        return False  # Windows installer package
    if url.endswith(".odt"):
        return False  # OpenOffice Writer document file
    if url.endswith(".ods"):
        return False  # OpenOffice Calc spreadsheet file
    if url.endswith(".oft"):
        return False  # Microsoft Outlook e-mail template file
    if url.endswith(".ogg"):
        return False  # Ogg Vorbis audio file
    if url.endswith(".ost"):
        return False  # Microsoft Outlook e-mail storage file
    if url.endswith(".otf"):
        return False  # Open type font file
    if url.endswith(".pkg"):
        return False  # Package file
    if url.endswith(".pdf"):
        return False  # Adobe PDF file
    if url.endswith(".png"):
        return False  # PNG image
    if url.endswith(".ppt") or url.endswith(".pptx"):
        return False  # Microsoft PowerPoint Open XML presentation
    if url.endswith(".ps"):
        return False  # PostScript file
    if url.endswith(".psd"):
        return False  # PSD image
    if url.endswith(".pst"):
        return False  # Microsoft Outlook e-mail storage file
    if url.endswith(".rar"):
        return False  # RAR file
    if url.endswith(".rpm"):
        return False  # Red Hat Package Manager
    if url.endswith(".rtf"):
        return False  # Rich Text Format file
    if url.endswith(".sql"):
        return False  # SQL database file
    if url.endswith(".svg"):
        return False  # Scalable Vector Graphics file
    if url.endswith(".swf"):
        return False  # Shockwave flash file
    if url.endswith(".xls") or url.endswith(".xlsx"):
        return False  # Microsoft Excel Open XML spreadsheet file
    if url.endswith(".toast"):
        return False  # Toast disc image
    if url.endswith(".tar"):
        return False  # Linux tarball file archive
    if url.endswith(".tar.gz"):
        return False  # Tarball compressed file
    if url.endswith(".tex"):
        return False  # A LaTeX document file
    if url.endswith(".ttf"):
        return False  # TrueType font file
    if url.endswith(".txt"):
        return False  # Plain text file
    if url.endswith(".tif") or url.endswith(".tiff"):
        return False  # TIFF image
    if url.endswith(".vcd"):
        return False  # Virtual CD
    if url.endswith(".vcf"):
        return False  # E-mail contact file
    if url.endswith(".vob"):
        return False  # DVD Video Object
    if url.endswith(".xml"):
        return False  # XML file
    if url.endswith(".wav") or url.endswith(".wma"):
        return False  # WAV file
    if url.endswith(".wmv"):
        return False  # Windows Media Video file
    if url.endswith(".wpd"):
        return False  # WordPerfect document
    if url.endswith(".wpl"):
        return False  # Windows Media Player playlist
    if url.endswith(".wsf"):
        return False  # Windows script file
    if url.endswith(".z") or url.endswith(".zip"):
        return False  # Z or Zip compressed file
    try:
        if not connection.is_connected():
            time.sleep(30)
            connection = mysql.connector.connect(
                host=HOSTNAME,
                database=DATABASE,
                user=USERNAME,
                password=PASSWORD,
                autocommit=True,
            )
            server_info = connection.get_server_info()
            print("MySQL connection is open on", server_info)
        sql_statement = (
            "UPDATE `url` SET `score` = `score` + 1 WHERE `address` = '%s'" % url
        )
        cursor = connection.cursor()
        cursor.execute(sql_statement)
        if cursor.rowcount == 0:
            sql_statement = (
                "INSERT INTO `url` (`address`,`visited`, `score`, `pagerank`) VALUES ('%s', FALSE, 1, 0.0)"
                % url
            )
            cursor.execute(sql_statement)
        cursor.close()
    except mysql.connector.Error as err:
        print("MySQL connector error:", str(err))
        return False
    finally:
        pass
    return True


def extract_url_from_frontier(connection):
    url = None
    try:
        if not connection.is_connected():
            time.sleep(30)
            connection = mysql.connector.connect(
                host=HOSTNAME,
                database=DATABASE,
                user=USERNAME,
                password=PASSWORD,
                autocommit=True,
            )
            server_info = connection.get_server_info()
            print("MySQL connection is open on", server_info)
        sql_statement = "SELECT `address` FROM `url` WHERE `visited` = FALSE ORDER BY `score` DESC, `url_id` ASC LIMIT 1"
        cursor = connection.cursor()
        cursor.execute(sql_statement)
        records = cursor.fetchall()
        for row in records:
            url = row[0]
            sql_statement = (
                "UPDATE `url` SET `visited` = TRUE WHERE `address` = '%s'" % url
            )
            cursor.execute(sql_statement)
        cursor.close()
    except mysql.connector.Error as err:
        print("MySQL connector error:", str(err))
    finally:
        pass
    return url


def add_url_to_database(connection, from_page, to_page):
    try:
        if not connection.is_connected():
            time.sleep(30)
            connection = mysql.connector.connect(
                host=HOSTNAME,
                database=DATABASE,
                user=USERNAME,
                password=PASSWORD,
                autocommit=True,
            )
            server_info = connection.get_server_info()
            print("MySQL connection is open on", server_info)
        # print("from_page = %s, to_page = %s" % (from_page, to_page))
        sql_statement = (
            "SET @from_page_id = (SELECT `url_id` FROM `url` WHERE `address` = '%s')"
            % from_page
        )
        cursor = connection.cursor()
        cursor.execute(sql_statement)
        if cursor.rowcount == 0:
            return
        sql_statement = (
            "SET @to_page_id = (SELECT `url_id` FROM `url` WHERE `address` = '%s')"
            % to_page
        )
        cursor.execute(sql_statement)
        if cursor.rowcount == 0:
            return
        sql_statement = "UPDATE `hyperlink` SET `counter` = `counter` + 1 WHERE `from_page` = @from_page_id AND `to_page` = @to_page_id"
        cursor.execute(sql_statement)
        if cursor.rowcount == 0:
            sql_statement = "INSERT INTO `hyperlink` (`from_page`, `to_page`, `counter`) VALUES (@from_page_id, @to_page_id, 1)"
            cursor.execute(sql_statement)
        cursor.close()
    except mysql.connector.Error as err:
        print("MySQL connector error:", str(err))
    finally:
        pass


def download_page_from_url(connection, url):
    html_title = None
    plain_text = None
    try:
        req = Request(url)
        html_page = urlopen(req)
        soup = BeautifulSoup(html_page, "html.parser")
        html_title = soup.title.get_text().strip()
        plain_text = soup.get_text().strip()
        plain_text = " ".join(plain_text.split())
        for hyperlink in soup.find_all("a"):
            hyperlink = urljoin(url, hyperlink.get("href"))
            if add_url_to_frontier(connection, hyperlink):
                add_url_to_database(connection, url, hyperlink)
    except urllib.error.URLError as err:
        print(str(err))
    except urllib.error.HTTPError as err:
        print(str(err))
    except urllib.error.ContentTooShortError as err:
        print(str(err))
    finally:
        return html_title, plain_text


def get_webpage_count(connection):
    counter = -1
    try:
        if not connection.is_connected():
            time.sleep(30)
            connection = mysql.connector.connect(
                host=HOSTNAME,
                database=DATABASE,
                user=USERNAME,
                password=PASSWORD,
                autocommit=True,
            )
            server_info = connection.get_server_info()
            print("MySQL connection is open on", server_info)
        sql_last_id = "SELECT COUNT(`webpage_id`) FROM `webpage`"
        cursor = connection.cursor()
        cursor.execute(sql_last_id)
        records = cursor.fetchone()
        counter = records[0]
        cursor.close()
    except mysql.connector.Error as err:
        print("MySQL connector error:", str(err))
        return -1
    finally:
        pass
    return counter


def web_search_engine():
    global webpage_count
    try:
        connection = mysql.connector.connect(
            host=HOSTNAME,
            database=DATABASE,
            user=USERNAME,
            password=PASSWORD,
            autocommit=True,
        )
        server_info = connection.get_server_info()
        print("MySQL connection is open on", server_info)
        webpage_count = get_webpage_count(connection)
        print("get_webpage_count = %d" % webpage_count)
        add_url_to_frontier(connection, "https://en.wikipedia.org/")
        while True:
            url = extract_url_from_frontier(connection)
            if url:
                print("Crawling %s... [%d]" % (url, webpage_count + 1))
                html_title, plain_text = download_page_from_url(connection, url)
                if html_title and plain_text:
                    if len(html_title) > 0:
                        connection = analyze_webpage(
                            connection, url, html_title, plain_text
                        )
            else:
                break
    except mysql.connector.Error as err:
        print("MySQL connector error:", str(err))
    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is now closed")


def analyze_webpage(connection, url, html_title, plain_text):
    global webpage_count
    while not connection.is_connected():
        try:
            time.sleep(30)
            connection = mysql.connector.connect(
                host=HOSTNAME,
                database=DATABASE,
                user=USERNAME,
                password=PASSWORD,
                autocommit=True,
            )
            server_info = connection.get_server_info()
            print("MySQL connection is open on", server_info)
        except mysql.connector.Error as err:
            print("MySQL connector error:", str(err))
        finally:
            pass
    try:
        sql_last_id = (
            "SET @last_url_id = (SELECT `url_id` FROM `url` WHERE `address` = '%s')"
            % url
        )
        cursor = connection.cursor()
        cursor.execute(sql_last_id)
        # html_title = html_title.encode(encoding='utf-8')
        # plain_text = plain_text.encode(encoding='utf-8')
        sql_statement = (
            "INSERT INTO `webpage` (`url_id`, `title`, `content`) VALUES (@last_url_id, '%s', '%s')"
            % (html_title.replace("'", '"'), plain_text.replace("'", '"'))
        )
        cursor.execute(sql_statement)
        if cursor.rowcount == 0:
            return connection
        sql_last_id = "SET @last_webpage_id = LAST_INSERT_ID()"
        cursor.execute(sql_last_id)
        cursor.close()
        webpage_count = webpage_count + 1
        return analyze_keyword(connection, plain_text)
    except mysql.connector.Error as err:
        print("MySQL connector error:", str(err))
    finally:
        pass
    return connection


def analyze_keyword(connection, plain_text):
    global webpage_count
    keyword_count = {}
    tokenize_list = tokenize(plain_text)
    for keyword in tokenize_list:
        if keyword.isascii() and keyword.isalnum():
            keyword = keyword.lower()
            if keyword_count.get(keyword) is None:
                keyword_count[keyword] = 1
            else:
                keyword_count[keyword] = keyword_count[keyword] + 1
    for keyword in keyword_count.keys():
        done = False
        while not done:
            try:
                if not connection.is_connected():
                    time.sleep(30)
                    connection = mysql.connector.connect(
                        host=HOSTNAME,
                        database=DATABASE,
                        user=USERNAME,
                        password=PASSWORD,
                        autocommit=True,
                    )
                    server_info = connection.get_server_info()
                    print("MySQL connection is open on", server_info)
                    sql_last_id = "SET @last_webpage_id = %d" % webpage_count
                    cursor = connection.cursor()
                    cursor.execute(sql_last_id)
                # keyword = keyword.encode(encoding='utf-8')
                sql_statement = (
                    "SELECT `keyword_id` FROM `keyword` WHERE `name` = '%s'" % keyword
                )
                cursor = connection.cursor()
                cursor.execute(sql_statement)
                records = cursor.fetchone()
                if cursor.rowcount == 0:
                    sql_statement = (
                        "INSERT INTO `keyword` (`name`) VALUES ('%s')" % keyword
                    )
                    cursor.execute(sql_statement)
                    sql_last_id = "SET @last_keyword_id = LAST_INSERT_ID()"
                    cursor.execute(sql_last_id)
                else:
                    sql_last_id = "SET @last_keyword_id = %d" % records[0]
                    cursor.execute(sql_last_id)
                sql_statement = (
                    "INSERT INTO `occurrence` (`webpage_id`, `keyword_id`, `counter`) VALUES "
                    "(@last_webpage_id, @last_keyword_id, %d)" % keyword_count[keyword]
                )
                cursor.execute(sql_statement)
                cursor.close()
                done = True
            except mysql.connector.Error as err:
                print("MySQL connector error:", str(err))
            finally:
                pass
    return connection


def pagerank(
    G,
    alpha=0.85,
    personalization=None,
    max_iter=100,
    tol=1.0e-6,
    nstart=None,
    weight="weight",
    dangling=None,
):
    """Return the PageRank of the nodes in the graph.

    PageRank computes a ranking of the nodes in the graph G based on
    the structure of the incoming links. It was originally designed as
    an algorithm to rank web pages.

    Parameters
    ----------
    G : graph
    A NetworkX graph. Undirected graphs will be converted to a directed
    graph with two directed edges for each undirected edge.

    alpha : float, optional
    Damping parameter for PageRank, default=0.85.

    personalization: dict, optional
    The "personalization vector" consisting of a dictionary with a
    key for every graph node and nonzero personalization value for each node.
    By default, a uniform distribution is used.

    max_iter : integer, optional
    Maximum number of iterations in power method eigenvalue solver.

    tol : float, optional
    Error tolerance used to check convergence in power method solver.

    nstart : dictionary, optional
    Starting value of PageRank iteration for each node.

    weight : key, optional
    Edge data key to use as weight. If None weights are set to 1.

    dangling: dict, optional
    The outedges to be assigned to any "dangling" nodes, i.e., nodes without
    any outedges. The dict key is the node the outedge points to and the dict
    value is the weight of that outedge. By default, dangling nodes are given
    outedges according to the personalization vector (uniform if not
    specified). This must be selected to result in an irreducible transition
    matrix (see notes under google_matrix). It may be common to have the
    dangling dict to be the same as the personalization dict.

    Returns
    -------
    pagerank : dictionary
    Dictionary of nodes with PageRank as value

    Notes
    -----
    The eigenvector calculation is done by the power iteration method
    and has no guarantee of convergence. The iteration will stop
    after max_iter iterations or an error tolerance of
    number_of_nodes(G)*tol has been reached.

    The PageRank algorithm was designed for directed graphs but this
    algorithm does not check if the input graph is directed and will
    execute on undirected graphs by converting each edge in the
    directed graph to two edges.
    """
    if len(G) == 0:
        return {}
    if not G.is_directed():
        D = G.to_directed()
    else:
        D = G
    # Create a copy in (right) stochastic form
    W = nx.stochastic_graph(D, weight=weight)
    N = W.number_of_nodes()
    # Choose fixed starting vector if not given
    if nstart is None:
        x = dict.fromkeys(W, 1.0 / N)
    else:
        # Normalized nstart vector
        s = float(sum(nstart.values()))
        x = dict((k, v / s) for k, v in nstart.items())
    if personalization is None:
        # Assign uniform personalization vector if not given
        p = dict.fromkeys(W, 1.0 / N)
    else:
        missing = set(G) - set(personalization)
        if missing:
            raise nx.NetworkXError(
                "Personalization dictionary "
                "must have a value for every node. "
                "Missing nodes %s" % missing
            )
        s = float(sum(personalization.values()))
        p = dict((k, v / s) for k, v in personalization.items())
    if dangling is None:
        # Use personalization vector if dangling vector not specified
        dangling_weights = p
    else:
        missing = set(G) - set(dangling)
        if missing:
            raise nx.NetworkXError(
                "Dangling node dictionary "
                "must have a value for every node. "
                "Missing nodes %s" % missing
            )
        s = float(sum(dangling.values()))
        dangling_weights = dict((k, v / s) for k, v in dangling.items())
    dangling_nodes = [n for n in W if W.out_degree(n, weight=weight) == 0.0]
    # power iteration: make up to max_iter iterations
    for _ in range(max_iter):
        xlast = x
        x = dict.fromkeys(xlast.keys(), 0)
        danglesum = alpha * sum(xlast[n] for n in dangling_nodes)
        for n in x:
            # this matrix multiply looks odd because it is
            # doing a left multiply x^T=xlast^T*W
            for nbr in W[n]:
                x[nbr] += alpha * xlast[n] * W[n][nbr][weight]
            x[n] += danglesum * dangling_weights[n] + (1.0 - alpha) * p[n]
        # check convergence, l1 norm
        err = sum([abs(x[n] - xlast[n]) for n in x])
        if err < N * tol:
            return x
    raise nx.NetworkXError(
        "pagerank: power iteration failed to converge " "in %d iterations." % max_iter
    )


# G = nx.barabasi_albert_graph(60, 41)
# pr = nx.pagerank(G, 0.4)
# print(pr)
if create_database():
    web_search_engine()
