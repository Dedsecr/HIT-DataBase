def get_books_info(connect, title, time, author, translator, press, price):
    index = {'title': title, 'time': time, 'author': author, 'translator': translator, 'press': press, 'price': price}
    all_none = True
    sql = 'SELECT * From view_books WHERE '
    for i in index:
        if index[i] != '':
            all_none = False
            sql += i + '=' + '\'' + index[i] + '\'' + ' AND '
    sql = sql[:-5]
    sql += ';'
    if all_none:
        sql = 'SELECT * From view_books;'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
    return result

def get_books_info_reviewer(connect, title, time, author, translator, press):
    index = {'title': title, 'time': time, 'author': author, 'translator': translator, 'press': press}
    all_none = True
    sql = 'SELECT * From view_books WHERE '
    for i in index:
        if index[i] != '':
            all_none = False
            sql += i + '=' + '\'' + index[i] + '\'' + ' AND '
    sql = sql[:-5]
    sql += ';'
    if all_none:
        sql = 'SELECT * From view_books;'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
    return result

def add_book(connect, title, time, author, translator, press, price):
    if title == '':
        raise Exception('Title cannot be empty')
    index = {'title': title, 'time': time, 'price': price}
    sql = 'INSERT INTO Book ('
    for i in index:
        if index[i] != '':
            sql += i + ', '
    sql = sql[:-2] + ') VALUES ('
    for i in index:
        if index[i] != '':
            sql += '\'' + index[i] + '\', '
    sql = sql[:-2] + ');'
    
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()
    
    if press != '':
        try:
            add_press(connect, press)
        except Exception as e:
            if 'Duplicate' not in e.__str__():
                raise e
        add_rel_publish(connect, title, press)
    if author != '':
        try:
            add_author(connect, author)
        except Exception as e:
            if 'Duplicate' not in e.__str__():
                raise e
        add_rel_write_book(connect, title, author)
    if translator != '':
        try:
            add_translator(connect, translator)
        except Exception as e:
            if 'Duplicate' not in e.__str__():
                raise e
        add_rel_translate_book(connect, title, translator)

def alter_book(connect, title, time, author, translator, press, price):
    if title == '':
        raise Exception('Title cannot be empty')
    result = get_books_info(connect, title, "", "", "", "", "")
    if len(result) == 0:
        raise Exception('Book not found')
    result = result[0]
    old_index = {'author': result[2], 'translator': result[3], 'press': result[4]}
    index = {'time': time, 'price': price}
    all_none = True
    sql = 'UPDATE Book SET '
    for i in index:
        if index[i] != '':
            all_none = False
            sql += i + '=' + '\'' + index[i] + '\', '
    sql = sql[:-2] + ' WHERE title=\'' + title + '\';'
    if not all_none:
        with connect.cursor() as cursor:
            cursor.execute(sql)
            connect.commit()
    
    if press != '':
        try:
            add_press(connect, press)
        except Exception as e:
            if 'Duplicate' not in e.__str__():
                raise e
        del_rel_publish(connect, title, old_index['press'])
        add_rel_publish(connect, title, press)
    if author != '':
        try:
            add_author(connect, author)
        except Exception as e:
            if 'Duplicate' not in e.__str__():
                raise e
        del_rel_write_book(connect, title, old_index['author'])
        add_rel_write_book(connect, title, author)
    if translator != '':
        try:
            add_translator(connect, translator)
        except Exception as e:
            if 'Duplicate' not in e.__str__():
                raise e
        del_rel_translate_book(connect, title, old_index['translator'])
        add_rel_translate_book(connect, title, translator)

def del_book(connect, title, time, author, translator, press, price):
    
    index = {'title': title, 'time': time, 'price': price}
    all_none = True
    sql = 'DELETE FROM Book WHERE '
    for i in index:
        if index[i] != '':
            all_none = False
            sql += i + '=' + '\'' + index[i] + '\'' + ' AND '
    sql = sql[:-5] + ';'
    if all_none:
        raise Exception('All Book selected')
    
    result = get_books_info(connect, title, time, author, translator, press, price)
    if len(result) == 0:
        raise Exception('Book not found')
    for i in result:
        del_rel_write_book(connect, i[1], i[2])
        del_rel_translate_book(connect, i[1], i[3])
        del_rel_publish(connect, i[1], i[4])

    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()

def add_press(connect, press):
    sql = 'INSERT INTO Press (name) VALUES (\'' + press + '\');'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()

def add_author(connect, author):
    sql = 'INSERT INTO Author (name) VALUES (\'' + author + '\');'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()
    
def add_translator(connect, translator):
    sql = 'INSERT INTO Translator (name) VALUES (\'' + translator + '\');'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()

def add_rel_publish(connect, book_name, press_name):
    # query for book_id and press_id
    sql = 'SELECT id FROM Book WHERE title=\'' + book_name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        book_id = cursor.fetchall()
    if len(book_id) == 0:
        raise Exception('Book not found')
    else:
        book_id = book_id[0][0]
    sql = 'SELECT id FROM Press WHERE name=\'' + press_name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        press_id = cursor.fetchall()
    if len(press_id) == 0:
        raise Exception('Press not found')
    else:
        press_id = press_id[0][0]
    # add relation
    sql = 'INSERT INTO Publish (book_id, press_id) VALUES (' + str(book_id) + ', ' + str(press_id) + ');'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()

def del_rel_publish(connect, book_name, press_name):
    # query for book_id and press_id
    sql = 'SELECT id FROM Book WHERE title=\'' + book_name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        book_id = cursor.fetchall()
    if len(book_id) == 0:
        raise Exception('Book not found')
    else:
        book_id = book_id[0][0]
    sql = 'SELECT id FROM Press WHERE name=\'' + press_name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        press_id = cursor.fetchall()
    if len(press_id) == 0:
        raise Exception('Press not found')
    else:
        press_id = press_id[0][0]
    # delete relation
    sql = 'DELETE FROM Publish WHERE book_id=' + str(book_id) + ' AND press_id=' + str(press_id) + ';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()

def add_rel_write_book(connect, book_name, author_name):
    # query for book_id and author_id
    sql = 'SELECT id FROM Book WHERE title=\'' + book_name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        book_id = cursor.fetchall()
    if len(book_id) == 0:
        raise Exception('Book not found')
    else:
        book_id = book_id[0][0]
    sql = 'SELECT id FROM Author WHERE name=\'' + author_name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        author_id = cursor.fetchall()
    if len(author_id) == 0:
        raise Exception('Author not found')
    else:
        author_id = author_id[0][0]
    # add relation
    sql = 'INSERT INTO WriteBook (book_id, author_id) VALUES (' + str(book_id) + ', ' + str(author_id) + ');'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()

def del_rel_write_book(connect, book_name, author_name):
    # query for book_id and author_id
    sql = 'SELECT id FROM Book WHERE title=\'' + book_name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        book_id = cursor.fetchall()
    if len(book_id) == 0:
        raise Exception('Book not found')
    else:
        book_id = book_id[0][0]
    sql = 'SELECT id FROM Author WHERE name=\'' + author_name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        author_id = cursor.fetchall()
    if len(author_id) == 0:
        raise Exception('Author not found')
    else:
        author_id = author_id[0][0]
    # delete relation
    sql = 'DELETE FROM WriteBook WHERE book_id=' + str(book_id) + ' AND author_id=' + str(author_id) + ';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()

def add_rel_translate_book(connect, book_name, translator_name):
    # query for book_id and translator_id
    sql = 'SELECT id FROM Book WHERE title=\'' + book_name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        book_id = cursor.fetchall()
    if len(book_id) == 0:
        raise Exception('Book not found')
    else:
        book_id = book_id[0][0]
    sql = 'SELECT id FROM Translator WHERE name=\'' + translator_name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        translator_id = cursor.fetchall()
    if len(translator_id) == 0:
        raise Exception('Translator not found')
    else:
        translator_id = translator_id[0][0]
    # add relation
    sql = 'INSERT INTO TranslateBook (book_id, translator_id) VALUES (' + str(book_id) + ', ' + str(translator_id) + ');'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()

def del_rel_translate_book(connect, book_name, translator_name):
    # query for book_id and translator_id
    sql = 'SELECT id FROM Book WHERE title=\'' + book_name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        book_id = cursor.fetchall()
    if len(book_id) == 0:
        raise Exception('Book not found')
    else:
        book_id = book_id[0][0]
    sql = 'SELECT id FROM Translator WHERE name=\'' + translator_name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        translator_id = cursor.fetchall()
    if len(translator_id) == 0:
        raise Exception('Translator not found')
    else:
        translator_id = translator_id[0][0]
    # delete relation
    sql = 'DELETE FROM TranslateBook WHERE book_id=' + str(book_id) + ' AND translator_id=' + str(translator_id) + ';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()

def get_reviews_info(connect, title, reviewer):
    index = {'title': title, 'reviewer': reviewer}
    all_none = True
    sql = 'SELECT * From view_reviews WHERE '
    for i in index:
        if index[i] != '':
            all_none = False
            sql += i + '=' + '\'' + index[i] + '\'' + ' AND '
    sql = sql[:-5]
    sql += ';'
    if all_none:
        sql = 'SELECT * From view_reviews;'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
    return result

def add_review(connect, title, reviewer, time, rating, body):
    # query for book_id and reviewer_id
    if title == '':
        raise Exception('Title cannot be empty')
    sql = 'SELECT id FROM Book WHERE title=\'' + title + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        book_id = cursor.fetchall()
    if len(book_id) == 0:
        raise Exception('Book not found')
    else:
        book_id = book_id[0][0]
    sql = 'SELECT id FROM Reviewer WHERE name=\'' + reviewer + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        reviewer_id = cursor.fetchall()
    if len(reviewer_id) == 0:
        add_reviewer(connect, reviewer, '')
        sql = 'SELECT id FROM Reviewer WHERE name=\'' + reviewer + '\';'
        with connect.cursor() as cursor:
            cursor.execute(sql)
            reviewer_id = cursor.fetchall()
    reviewer_id = reviewer_id[0][0]
    # add body
    sql = 'INSERT INTO Review (book_id, reviewer_id, time, rating, body) VALUES (' + str(book_id) + ', ' + str(reviewer_id) + ', \'' + time + '\', ' + str(rating) + ', \'' + body + '\');'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()

def del_review(connect, title, reviewer):
    # query for book_id and reviewer_id
    if title == '' and reviewer == '':
        raise Exception('Title and reviewer cannot be empty in the same time')
    book_id, reviewer_id = '', ''
    if title != '':
        sql = 'SELECT id FROM Book WHERE title=\'' + title + '\';'
        with connect.cursor() as cursor:
            cursor.execute(sql)
            book_id = cursor.fetchall()
        if len(book_id) == 0:
            raise Exception('Book not found')
        else:
            book_id = book_id[0][0]
    if reviewer != '':
        sql = 'SELECT id FROM Reviewer WHERE name=\'' + reviewer + '\';'
        with connect.cursor() as cursor:
            cursor.execute(sql)
            reviewer_id = cursor.fetchall()
        if len(reviewer_id) == 0:
            raise Exception('Reviewer not found')
        else:
            reviewer_id = reviewer_id[0][0]
    
    index = {'book_id': book_id, 'reviewer_id': reviewer_id}
    sql = 'SELECT review_id FROM Review WHERE '
    for i in index:
        if index[i] != '':
            sql += i + '=' + str(index[i]) + ' AND '
    sql = sql[:-5]
    sql += ';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        review_id = cursor.fetchall()
    for i in review_id:
        del_like_review_by_review_id(connect, i[0])

    # delete body
    sql = 'DELETE FROM Review WHERE '
    for i in index:
        if index[i] != '':
            sql += i + '=' + str(index[i]) + ' AND '
    sql = sql[:-5]
    sql += ';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()

def like_review(connect, review_id, reviewer):
    # query for reviewer_id
    sql = 'SELECT id FROM Reviewer WHERE name=\'' + reviewer + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        reviewer_id = cursor.fetchall()
    if len(reviewer_id) == 0:
        add_reviewer(connect, reviewer, '')
        sql = 'SELECT id FROM Reviewer WHERE name=\'' + reviewer + '\';'
        with connect.cursor() as cursor:
            cursor.execute(sql)
            reviewer_id = cursor.fetchall()
    reviewer_id = reviewer_id[0][0]
    sql = 'SELECT review_id FROM Review WHERE review_id=' + str(review_id) + ';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        review_id_ = cursor.fetchall()
    if len(review_id_) == 0:
        raise Exception('Review not found')
    # add like
    sql = 'INSERT INTO LikeReview (review_id, reviewer_id) VALUES (' + str(review_id) + ', ' + str(reviewer_id) + ');'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()

def del_like_review_by_review_id(connect, review_id):
    sql = 'DELETE FROM LikeReview WHERE review_id=' + str(review_id) + ';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()

def del_like_review_by_reviewer_id(connect, reviewer_id):
    sql = 'DELETE FROM LikeReview WHERE review_id=' + str(reviewer_id) + ';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()

def get_reviewers_info(connect, name):
    sql = 'SELECT * From view_follow WHERE '
    sql += 'name=\'' + name + '\';'
    if name == '':
        sql = 'SELECT * From view_follow;'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
    return result

def add_reviewer(connect, name, gender):
    if name == '':
        raise Exception('Name cannot be empty')
    index = {'name': name, 'gender': gender}
    sql = 'INSERT INTO Reviewer ('
    for i in index:
        if index[i] != '':
            sql += i + ', '
    sql = sql[:-2] + ') VALUES ('
    for i in index:
        if index[i] != '':
            sql += '\'' + index[i] + '\', '
    sql = sql[:-2] + ');'
    
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()

def del_reviewer(connect, name):
    if name == '':
        raise Exception('Name cannot be empty')
    result = get_reviews_info(connect, '', name)
    for i in result:
        del_review(connect, i[1], name)

    sql = 'SELECT id FROM Reviewer WHERE name=\'' + name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        review_id = cursor.fetchall()
    for i in review_id:
        del_like_review_by_reviewer_id(connect, i[0])

    sql = 'DELETE FROM Reviewer WHERE name=\'' + name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()

def get_follow_info(connect, name):
    sql = 'SELECT * From view_follow WHERE '
    if name == '':
        raise Exception('Name cannot be empty')
    sql += 'name=\'' + name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
    return result

def follow_reviewer(connect, fed_name, fing_name):
    if fed_name == '' or fing_name == '':
        raise Exception('Name cannot be empty')
    sql = 'SELECT id FROM Reviewer WHERE name=\'' + fing_name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        fing_reviewer_id = cursor.fetchall()
    if len(fing_reviewer_id) == 0:
        add_reviewer(connect, fing_name, '')
        sql = 'SELECT id FROM Reviewer WHERE name=\'' + fing_name + '\';'
        with connect.cursor() as cursor:
            cursor.execute(sql)
            fing_reviewer_id = cursor.fetchall()
    fing_reviewer_id = fing_reviewer_id[0][0]
    sql = 'SELECT id FROM Reviewer WHERE name=\'' + fed_name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        fed_reviewer_id = cursor.fetchall()
    if len(fed_reviewer_id) == 0:
        raise Exception('Reviewer not found')
    fed_reviewer_id = fed_reviewer_id[0][0]
    sql = 'INSERT INTO Follow (fed_id, fing_id) VALUES ( ' + str(fed_reviewer_id) + ', ' + str(fing_reviewer_id) + ');'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()

def unfollow_reviewer(connect, fed_name, fing_name):
    if fed_name == '' or fing_name == '':
        raise Exception('Name cannot be empty')
    sql = 'SELECT id FROM Reviewer WHERE name=\'' + fing_name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        fing_reviewer_id = cursor.fetchall()
    if len(fing_reviewer_id) == 0:
        add_reviewer(connect, fing_name, '')
        sql = 'SELECT id FROM Reviewer WHERE name=\'' + fing_name + '\';'
        with connect.cursor() as cursor:
            cursor.execute(sql)
            fing_reviewer_id = cursor.fetchall()
    fing_reviewer_id = fing_reviewer_id[0][0]
    sql = 'SELECT id FROM Reviewer WHERE name=\'' + fed_name + '\';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        fed_reviewer_id = cursor.fetchall()
    if len(fed_reviewer_id) == 0:
        raise Exception('Reviewer not found')
    fed_reviewer_id = fed_reviewer_id[0][0]
    sql = 'DELETE FROM Follow WHERE fed_id=' + str(fed_reviewer_id) + ' AND fing_id=' + str(fing_reviewer_id) + ';'
    with connect.cursor() as cursor:
        cursor.execute(sql)
        connect.commit()