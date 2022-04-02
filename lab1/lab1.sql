(SELECT DISTINCT maker FROM Product WHERE type = 'laptop') MINUS (SELECT DISTINCT maker FROM Product WHERE type = 'pc');

SELECT DISTINCT maker FROM Product WHERE type = 'laptop' AND maker NOT IN (SELECT DISTINCT maker FROM Product WHERE type = 'pc');

SELECT DISTINCT maker FROM Product AS S WHERE type = 'laptop' AND NOT EXISTS (SELECT * FROM Product AS T WHERE T.type = 'pc' AND S.maker = T.maker);


SELECT P1.model FROM Printer AS P1 JOIN Printer AS P2 on (P1.price < P2.price) AND P2.model = '3002';

SELECT model FROM Printer WHERE price < (SELECT price FROM Printer WHERE model = '3002');

SELECT model FROM Printer AS P1 WHERE EXISTS (SELECT * FROM Printer AS P2 WHERE P2.model = '3002' AND P1.price < P2.price);


SELECT DISTINCT P1.model FROM PC AS P1 LEFT JOIN PC AS P2 ON P1.speed < P2.speed WHERE P2.model is NULL;

SELECT model FROM PC WHERE speed IN (SELECT MAX(speed) FROM PC);

SELECT model FROM PC WHERE speed = (SELECT MAX(speed) FROM PC);

SELECT model FROM PC WHERE speed >= ALL (SELECT speed FROM PC);

SELECT model FROM PC AS P1 WHERE NOT EXISTS (SELECT * FROM PC AS P2 WHERE P2.speed > P1.speed);


SELECT DISTINCT P1.maker FROM ((Product AS P1 NATURAL JOIN PC AS PC1) JOIN (Product AS P2 NATURAL JOIN PC AS PC2) ON (P1.maker = P2.maker) JOIN (Product AS P3 NATURAL JOIN PC AS PC3) ON (P1.maker = P3.maker)) WHERE PC1.speed != PC2.speed AND PC1.speed != PC3.speed AND PC2.speed != PC3.speed;

SELECT maker FROM Product NATURAL JOIN PC GROUP BY maker HAVING COUNT(DISTINCT speed) >= 3;

SELECT maker FROM (SELECT maker, COUNT(DISTINCT speed) AS c FROM Product NATURAL JOIN PC GROUP BY maker) AS P WHERE P.c >= 3;


UPDATE PC SET price = price * 0.9 WHERE 'A' = (SELECT maker FROM Product WHERE PC.model = Product.model);

UPDATE PC SET price = price * 0.9 WHERE model IN (SELECT model FROM Product WHERE maker = 'A');

UPDATE PC AS P1 SET price = price * 0.9 WHERE EXISTS (SELECT * FROM Product AS P2 WHERE P1.model = P2.model AND P2.maker = 'A');