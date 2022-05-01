DROP DATABASE IF EXISTS BookReview;
CREATE DATABASE BookReview;
USE BookReview;

CREATE TABLE Book (
    `id` INT AUTO_INCREMENT,
    `title` VARCHAR(128) NOT NULL,
    `time` YEAR,
    `price` DECIMAL(8, 2),
    PRIMARY KEY (`id`),
    UNIQUE KEY `title` (`title`)
);

CREATE TABLE Press (
    `id` INT AUTO_INCREMENT,
    `name` VARCHAR(128) NOT NULL,
    `location` VARCHAR(128),
    PRIMARY KEY (`id`),
    UNIQUE KEY `name` (`name`)
);

CREATE TABLE Author (
    `id` INT AUTO_INCREMENT,
    `name` VARCHAR(128) NOT NULL,
    `country` VARCHAR(128),
    `gender` ENUM('F', 'M', 'UNK') DEFAULT 'UNK',
    PRIMARY KEY (`id`),
    UNIQUE KEY `name` (`name`)
);

CREATE TABLE Translator (
    `id` INT AUTO_INCREMENT,
    `name` VARCHAR(128) NOT NULL,
    `country` VARCHAR(128),
    `gender` ENUM('F', 'M', 'UNK') DEFAULT 'UNK',
    PRIMARY KEY (`id`),
    UNIQUE KEY `name` (`name`)
);

CREATE TABLE Reviewer(
    `id` INT AUTO_INCREMENT,
    `name` VARCHAR(128) NOT NULL,
    `gender` ENUM('F', 'M', 'UNK') DEFAULT 'UNK',
    PRIMARY KEY(`id`),
    UNIQUE KEY `name` (`name`)
);

CREATE TABLE Publish(
	`book_id` INT,
    `press_id` INT,
    PRIMARY KEY(`book_id`, `press_id`),
    FOREIGN KEY(`book_id`) REFERENCES Book(`id`) ON DELETE CASCADE,
    FOREIGN KEY(`press_id`) REFERENCES Press(`id`) ON DELETE CASCADE
);

CREATE TABLE WriteBook(
	`book_id` INT,
    `author_id` INT,
    PRIMARY KEY(`book_id`, `author_id`),
    FOREIGN KEY(`book_id`) REFERENCES Book(`id`) ON DELETE CASCADE,
    FOREIGN KEY(`author_id`) REFERENCES Author(`id`) ON DELETE CASCADE
);

CREATE TABLE TranslateBook(
	`book_id` INT,
    `translator_id` INT,
    PRIMARY KEY(`book_id`, `translator_id`),
    FOREIGN KEY(`book_id`) REFERENCES Book(`id`) ON DELETE CASCADE,
    FOREIGN KEY(`translator_id`) REFERENCES Translator(`id`) ON DELETE CASCADE
);

CREATE TABLE Review(
    `review_id` INT AUTO_INCREMENT,
	`book_id` INT,
    `reviewer_id` INT,
    `time` DATE NOT NULL,
    `rating` INT NOT NULL CHECK (rating >= 1 AND rating <= 5),
    `body` TEXT NOT NULL,
    PRIMARY KEY(`review_id`),
    FOREIGN KEY(`book_id`) REFERENCES Book(`id`) ON DELETE CASCADE,
	FOREIGN KEY(`reviewer_id`) REFERENCES Reviewer(`id`) ON DELETE CASCADE
);

CREATE TABLE LikeReview(
	`reviewer_id` INT,
    `review_id` INT,
    PRIMARY KEY(`reviewer_id`, `review_id`),
    FOREIGN KEY(`reviewer_id`) REFERENCES Reviewer(`id`) ON DELETE CASCADE,
	FOREIGN KEY(`review_id`) REFERENCES Review(`review_id`) ON DELETE CASCADE
);

CREATE TABLE Follow(
	`fed_id` INT,
    `fing_id` INT,
    PRIMARY KEY(`fed_id`, `fing_id`),
    FOREIGN KEY(`fed_id`) REFERENCES Reviewer(`id`) ON DELETE CASCADE,
	FOREIGN KEY(`fing_id`) REFERENCES Reviewer(`id`) ON DELETE CASCADE
);


CREATE VIEW view_books AS SELECT Book.id, Book.title, Author.name AS author, Translator.name AS translator, Press.name AS press, Book.time, Book.price, AVG(Review.rating) FROM Book LEFT JOIN WriteBook ON WriteBook.book_id=Book.id LEFT JOIN Author ON Author.id=WriteBook.author_id LEFT JOIN TranslateBook ON TranslateBook.book_id=Book.id LEFT JOIN Translator ON Translator.id=TranslateBook.translator_id LEFT JOIN Publish ON Publish.book_id=Book.id LEFT JOIN Press ON Press.id=Publish.press_id LEFT JOIN Review ON Review.book_id=Book.id GROUP BY Book.id, Book.title, author, translator, press, Book.time, Book.price;

CREATE VIEW view_reviews AS SELECT Review.review_id, Book.title AS title, Reviewer.name AS reviewer, Review.time, Review.rating, Review.body, COUNT(LikeReview.reviewer_id) AS like_num FROM Review LEFT JOIN Reviewer ON Reviewer.id=Review.reviewer_id LEFT JOIN Book ON Book.id=Review.book_id LEFT JOIN LikeReview ON LikeReview.review_id=Review.review_id GROUP BY Review.review_id;

-- for view view_follow
CREATE VIEW view_followed_num AS SELECT fed_Reviewer.id AS fed_id, fed_Reviewer.name AS fed_name, fed_Reviewer.gender AS fed_gender, COUNT(fing_Reviewer.name) AS fed_num FROM Follow RIGHT JOIN Reviewer AS fed_Reviewer ON fed_Reviewer.id=Follow.fed_id LEFT JOIN Reviewer AS fing_Reviewer ON fing_Reviewer.id=Follow.fing_id GROUP BY fed_Reviewer.name;

-- for view view_follow
CREATE VIEW view_following_num AS SELECT fing_Reviewer.id AS fing_id, fing_Reviewer.name AS fing_name, fing_Reviewer.gender AS fing_gender, COUNT(fed_Reviewer.name) AS fing_num FROM Follow RIGHT JOIN Reviewer AS fing_Reviewer ON fing_Reviewer.id=Follow.fing_id LEFT JOIN Reviewer AS fed_Reviewer ON fed_Reviewer.id=Follow.fed_id GROUP BY fing_Reviewer.name;

CREATE VIEW view_follow AS SELECT fed_id AS id, fed_name AS name, fed_gender AS gender, fed_num, fing_num FROM view_followed_num FULL JOIN view_following_num ON fed_id=fing_id;
