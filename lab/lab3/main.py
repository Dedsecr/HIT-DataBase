import sys
import gui.Ui_login, gui.Ui_admin, gui.Ui_user
from PyQt5.QtWidgets import QApplication, QDialog, QHeaderView, QTableWidgetItem, QMessageBox
import pymysql
import sql.query as query
import time as TIME

class LoginDialog(QDialog):
    def __init__(self, admin_dialog, user_dialog):
        super(QDialog, self).__init__()
        self.ui = gui.Ui_login.Ui_Login()
        self.ui.setupUi(self)
        self.ui.pushButton.clicked.connect(self.login_show)
        self.admin_dialog = admin_dialog
        self.user_dialog = user_dialog

    def login_show(self):
        if self.ui.lineEdit.text() == '':
            QMessageBox.warning(self, 'Error', 'username is empty!')
            return
        self.close()
        if self.ui.lineEdit.text() == 'r00t':
            self.admin_dialog.show()
        else:
            self.user_dialog.show()
            self.user_dialog.ui.label_name.setText(self.ui.lineEdit.text())

class AdminDialog(QDialog):
    def __init__(self, connect: pymysql.connections.Connection):
        super(QDialog, self).__init__()
        self.ui = gui.Ui_admin.Ui_Admin()
        self.ui.setupUi(self)
        self.ui.tableWidget_book.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.ui.tableWidget_review.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.ui.tableWidget_reviewer.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.connect = connect
        self.ui.pushButton_book_search.clicked.connect(self.book_search)
        self.ui.pushButton_book_add.clicked.connect(self.book_add)
        self.ui.pushButton_book_alter.clicked.connect(self.book_alter)
        self.ui.pushButton_book_del.clicked.connect(self.book_del)
        self.ui.pushButton_review_search.clicked.connect(self.review_search)
        self.ui.pushButton_review_del.clicked.connect(self.review_del)
        self.ui.pushButton_reviewer_search.clicked.connect(self.reviewer_search)
        self.ui.pushButton_reviewer_del.clicked.connect(self.reviewer_del)
    
    def _get_book_table_info(self):
        title = self.ui.lineEdit_book_title.text()
        time = self.ui.lineEdit_book_time.text()
        author = self.ui.lineEdit_book_author.text()
        translator = self.ui.lineEdit_book_translator.text()
        press = self.ui.lineEdit_book_press.text()
        price = self.ui.lineEdit_book_price.text()
        return title, time, author, translator, press, price
    
    def _book_show(self, result):
        self.ui.tableWidget_book.setRowCount(len(result))
        for row in range(len(result)):
            for column in range(8):
                item = QTableWidgetItem(str(result[row][column]))
                self.ui.tableWidget_book.setItem(row, column, item)
    
    def _get_review_table_info(self):
        # Review.review_id, title, reviewer, Review.time, Review.rating, Review.body
        title = self.ui.lineEdit_review_title.text()
        reviewer = self.ui.lineEdit_review_reviewer.text()
        return title, reviewer
    
    def _review_show(self, result):
        self.ui.tableWidget_review.setRowCount(len(result))
        for row in range(len(result)):
            for column in range(7):
                item = QTableWidgetItem(str(result[row][column]))
                self.ui.tableWidget_review.setItem(row, column, item)
    
    def _reviewer_show(self, result):
        self.ui.tableWidget_reviewer.setRowCount(len(result))
        for row in range(len(result)):
            for column in range(5):
                item = QTableWidgetItem(str(result[row][column]))
                self.ui.tableWidget_reviewer.setItem(row, column, item)
    
    def book_search(self):
        self.ui.tableWidget_book.clearContents()
        title, time, author, translator, press, price = self._get_book_table_info()
        result = query.get_books_info(self.connect, title, time, author, translator, press, price)
        self._book_show(result)
    
    def book_add(self):
        self.ui.tableWidget_book.clearContents()
        title, time, author, translator, press, price = self._get_book_table_info()

        try:
            query.add_book(self.connect, title, time, author, translator, press, price)
            QMessageBox.information(self, 'Warning', 'Add book successfully!')
        except Exception as e:
            self.ui.tableWidget_book.setRowCount(0)
            if 'Duplicate' not in e.__str__():
                QMessageBox.information(self, 'Warning', e.__str__())
            return
        result = query.get_books_info(self.connect, title, time, author, translator, press, price)
        self._book_show(result)
        
    def book_alter(self):
        self.ui.tableWidget_book.clearContents()
        title, time, author, translator, press, price = self._get_book_table_info()
        try:
            query.alter_book(self.connect, title, time, author, translator, press, price)
            QMessageBox.information(self, 'Warning', 'Alter book successfully!')
        except Exception as e:
            self.ui.tableWidget_book.setRowCount(0)
            if 'Duplicate' not in e.__str__():
                QMessageBox.information(self, 'Warning', e.__str__())
            return
        result = query.get_books_info(self.connect, title, time, author, translator, press, price)
        self._book_show(result)
    
    def book_del(self):
        self.ui.tableWidget_book.clearContents()
        title, time, author, translator, press, price = self._get_book_table_info()
        try:
            query.del_book(self.connect, title, time, author, translator, press, price)
            QMessageBox.information(self, 'Warning', 'Delete book successfully!')
        except Exception as e:
            self.ui.tableWidget_book.setRowCount(0)
            if 'Duplicate' not in e.__str__():
                QMessageBox.information(self, 'Warning', e.__str__())
            return
        result = query.get_books_info(self.connect, title, time, author, translator, press, price)
        self._book_show(result)
    
    def review_search(self):
        self.ui.tableWidget_review.clearContents()
        title, reviewer = self._get_review_table_info()
        result = query.get_reviews_info(self.connect, title, reviewer)
        self._review_show(result)
    
    def review_del(self):
        self.ui.tableWidget_review.clearContents()
        title, reviewer = self._get_review_table_info()
        try:
            query.del_review(self.connect, title, reviewer)
            QMessageBox.information(self, 'Warning', 'Delete review successfully!')
        except Exception as e:
            self.ui.tableWidget_review.setRowCount(0)
            if 'Duplicate' not in e.__str__():
                QMessageBox.information(self, 'Warning', e.__str__())
            return
        result = query.get_reviews_info(self.connect, title, reviewer)
        self._review_show(result)
    
    def reviewer_search(self):
        self.ui.tableWidget_reviewer.clearContents()
        name = self.ui.lineEdit_reviewer_reviewer.text()
        result = query.get_reviewers_info(self.connect, name)
        self._reviewer_show(result)
    
    def reviewer_del(self):
        self.ui.tableWidget_reviewer.clearContents()
        name = self.ui.lineEdit_reviewer_reviewer.text()
        try:
            query.del_reviewer(self.connect, name)
            QMessageBox.information(self, 'Warning', 'Delete reviewer successfully!')
        except Exception as e:
            self.ui.tableWidget_reviewer.setRowCount(0)
            if 'Duplicate' not in e.__str__():
                QMessageBox.information(self, 'Warning', e.__str__())
            return
        result = query.get_reviewers_info(self.connect, name)
        self._reviewer_show(result)

class UserDialog(QDialog):
    def __init__(self, connect: pymysql.connections.Connection):
        super(QDialog, self).__init__()
        self.ui = gui.Ui_user.Ui_User()
        self.ui.setupUi(self)
        self.ui.label_name.setText('')
        self.ui.tableWidget_book.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.ui.tableWidget_review.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.ui.tableWidget_follow.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.connect = connect
        self.ui.pushButton_book_search.clicked.connect(self.book_search)
        self.ui.pushButton_review_search_search.clicked.connect(self.review_search)
        self.ui.pushButton_review_add_review_add.clicked.connect(self.review_add)
        self.ui.pushButton_review_like_review.clicked.connect(self.review_like)
        self.ui.pushButton_follow_search.clicked.connect(self.follow_search)
        self.ui.pushButton_follow_follow.clicked.connect(self.follow_follow)
        self.ui.pushButton_follow_unfollow.clicked.connect(self.follow_unfollow)
    
    def _get_book_table_info(self):
        title = self.ui.lineEdit_book_title.text()
        time = self.ui.lineEdit_book_time.text()
        author = self.ui.lineEdit_book_author.text()
        translator = self.ui.lineEdit_book_translator.text()
        press = self.ui.lineEdit_book_press.text()
        return title, time, author, translator, press
    
    def _book_show(self, result):
        self.ui.tableWidget_book.setRowCount(len(result))
        for row in range(len(result)):
            for column in range(8):
                item = QTableWidgetItem(str(result[row][column]))
                self.ui.tableWidget_book.setItem(row, column, item)
    
    def _get_review_table_info(self):
        # Review.review_id, title, reviewer, Review.time, Review.rating, Review.body
        title = self.ui.lineEdit_review_search_title.text()
        reviewer = self.ui.lineEdit_review_search_reviewer.text()
        return title, reviewer
    
    def _get_add_review_info(self):
        title = self.ui.lineEdit_review_add_review_title.text()
        reviewer = self.ui.label_name.text()
        time = TIME.strftime("%Y-%m-%d", TIME.localtime())
        rating = self.ui.spinBox_add_review_rating.value()
        body = self.ui.plainTextEdit_add_review_review.toPlainText()
        return title, reviewer, time, rating, body
    
    def _review_show(self, result):
        self.ui.tableWidget_review.setRowCount(len(result))
        for row in range(len(result)):
            for column in range(7):
                item = QTableWidgetItem(str(result[row][column]))
                self.ui.tableWidget_review.setItem(row, column, item)
    
    def _reviewer_show(self, result):
        self.ui.tableWidget_follow.setRowCount(len(result))
        for row in range(len(result)):
            for column in range(5):
                item = QTableWidgetItem(str(result[row][column]))
                self.ui.tableWidget_follow.setItem(row, column, item)
    
    def book_search(self):
        self.ui.tableWidget_book.clearContents()
        title, time, author, translator, press = self._get_book_table_info()
        result = query.get_books_info_reviewer(self.connect, title, time, author, translator, press)
        self._book_show(result)

    def review_search(self):
        self.ui.tableWidget_review.clearContents()
        title, reviewer = self._get_review_table_info()
        result = query.get_reviews_info(self.connect, title, reviewer)
        self._review_show(result)
    
    def review_add(self):
        self.ui.tableWidget_review.clearContents()
        title, reviewer, time, rating, body = self._get_add_review_info()
        try:
            query.add_review(self.connect, title, reviewer, time, rating, body)
            QMessageBox.information(self, 'Warning', 'Add review successfully!')
        except Exception as e:
            self.ui.tableWidget_review.setRowCount(0)
            if 'Duplicate' not in e.__str__():
                QMessageBox.information(self, 'Warning', e.__str__())
            return
        result = query.get_reviews_info(self.connect, title, reviewer)
        self._review_show(result)
    
    def review_like(self):
        review_id = self.ui.lineEdit_review_review_id.text()
        reviewer = self.ui.label_name.text()
        try:
            query.like_review(self.connect, review_id, reviewer)
            QMessageBox.information(self, 'Warning', 'Like review successfully!')
        except Exception as e:
            self.ui.tableWidget_review.setRowCount(0)
            if 'Duplicate' not in e.__str__():
                QMessageBox.information(self, 'Warning', e.__str__())
            return

    def follow_search(self):
        self.ui.tableWidget_follow.clearContents()
        fed_name = self.ui.lineEdit_follow_reviewer.text()
        try:
            result = query.get_follow_info(self.connect, fed_name)
        except Exception as e:
            self.ui.tableWidget_review.setRowCount(0)
            if 'Duplicate' not in e.__str__():
                QMessageBox.information(self, 'Warning', e.__str__())
            return
        self._reviewer_show(result)
    
    def follow_follow(self):
        fing_name = self.ui.label_name.text()
        fed_name = self.ui.lineEdit_follow_reviewer.text()
        try:
            query.follow_reviewer(self.connect, fed_name, fing_name)
            QMessageBox.information(self, 'Warning', 'Follow successfully!')
        except Exception as e:
            self.ui.tableWidget_review.setRowCount(0)
            if 'Duplicate' not in e.__str__():
                QMessageBox.information(self, 'Warning', e.__str__())
            return
    
    def follow_unfollow(self):
        fing_name = self.ui.label_name.text()
        fed_name = self.ui.lineEdit_follow_reviewer.text()
        try:
            query.unfollow_reviewer(self.connect, fed_name, fing_name)
            QMessageBox.information(self, 'Warning', 'Unfollow successfully!')
        except Exception as e:
            self.ui.tableWidget_review.setRowCount(0)
            if 'Duplicate' not in e.__str__():
                QMessageBox.information(self, 'Warning', e.__str__())
            return

if __name__ == '__main__':
    connect = pymysql.connect(host='localhost', port=3306, charset='utf8', user='root', password='123456', database='BookReview')
    app = QApplication(sys.argv)
    admin_dialog = AdminDialog(connect)
    user_dialog = UserDialog(connect)
    login_dialog = LoginDialog(admin_dialog, user_dialog)
    login_dialog.show()


    sys.exit(app.exec_())