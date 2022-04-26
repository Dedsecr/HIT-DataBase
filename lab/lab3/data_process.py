
from sql.query import *
import pymysql
from tqdm import tqdm, trange

connect = pymysql.connect(host='localhost', port=3306, charset='utf8', user='root', password='123456', database='BookReview')

def book1():
    f_intro = open('./data/doubanbook_top250_introduction.txt', 'r', encoding='utf8')
    intro_lines = f_intro.readlines()[1:]
    for i in trange(len(intro_lines)):
        intro_line = intro_lines[i]
        intro_data = intro_line.split('\t')
        intro_dict = {}
        title = intro_data[0].strip()
        if '                  ' in title:
            title = title.split(':')[0].strip()
        
        if '] ' in intro_data[1]:
            author = intro_data[1].split('] ')[1].strip()
            country = intro_data[1].split('] ')[0].split('[')[1].strip()
        else:
            author = intro_data[1].strip()
            country = '中'
        
        translator = intro_data[2].strip()
        press = intro_data[3].strip()
        p_time = intro_data[4].strip()
        price = intro_data[5].strip()
        if '元' in price:
            price = price.replace('元', '').strip()
        intro_dict['country'] = country
        try:
            add_book(connect, title, p_time, author, translator, press, price)
            sql = 'UPDATE Author SET country = "%s" WHERE name = "%s"' % (country, author)
            connect.cursor().execute(sql)
            connect.commit()
            sql = 'UPDATE Translator SET country = "%s" WHERE name = "%s"' % ('中', translator)
            connect.cursor().execute(sql)
            connect.commit()
        except:
            pass


def com():
    f_comment = open('./data/doubanbook_top250_comments.txt', 'r', encoding='utf8')
    comment_lines = f_comment.readlines()[1:]
    comment_datas = []
    for i in trange(len(comment_lines)):
        comment_line = comment_lines[i]
        comment_data = comment_line.split('\t')
        comment_dict = {}
        try:
            book = comment_data[0].strip()
            id = comment_data[1].strip()
            rating = comment_data[2].strip()
            if rating == 'none':
                rating = 3
            else:
                rating = int(rating.replace('allstar', '').strip()) // 10
            time = comment_data[3].strip()
            likenum = comment_data[4].strip()
            body = comment_data[5].strip()
            try:
                add_review(connect, book, id, time, rating, body)
            except:
                pass
        except:
            continue
        comment_datas.append(comment_dict)

if __name__ == '__main__':
    book1()
    com()