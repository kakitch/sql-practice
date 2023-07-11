ABORT;
drop table category_rev_det;
drop table category_rev_sum;


--Part B: transformation function
CREATE OR REPLACE FUNCTION was_returned(return_date timestamp without time zone)
	RETURNS varchar(10)
	LANGUAGE plpgsql
AS
$$

DECLARE	v_dt timestamp without time zone;
DECLARE	v_answer varchar(10);
BEGIN
	SELECT return_date INTO v_dt;
	if v_dt ISNULL THEN
	    v_answer = 'No';
	else 
		v_answer = 'Yes';
	end if;
	RETURN v_answer;
END; 
$$
;

--part C: Create tables
CREATE Table if not exists category_rev_det(
	category_id integer, 
	category_name varchar(25),
	title varchar(255),
	was_returned varchar(10),
	replacement_cost numeric(5,2),
	amount numeric(5,2)
);	



CREATE TABLE if not exists category_rev_sum(
	category_id integer,
	category_name varchar(25),
	revenue numeric(10,2),
	cost_of_unreturned numeric(10,2),
	category_profit numeric (10,2),
	category_init_cost numeric(10,2),
	roi numeric (10,1)
);	






--part E: create trigger function to update the Summary table

CREATE TRIGGER fill_summary_trigger
   AFTER INSERT OR UPDATE
   ON category_rev_det
   FOR EACH STATEMENT 
       EXECUTE PROCEDURE fill_summary();	
	   
CREATE OR REPLACE FUNCTION fill_summary()
	RETURNS TRIGGER
	LANGUAGE PLPGSQL
AS
$$
BEGIN

TRUNCATE TABLE category_rev_sum;
INSERT INTO category_rev_sum
SELECT t1.category_id, t1.category_name, t1.sum, t3.sum, t1.sum - t3.sum, t4.sum, ((t1.sum - t3.sum)/t4.sum)*100
from (SELECT category_id, category_name, COUNT(title), SUM(amount) from category_rev_det GROUP BY category_id, category_name)
	  AS t1
Join (SELECT category_id, COUNT(was_returned) from category_rev_det WHERE was_returned = 'No' GROUP BY category_id)
      AS t2
ON t1.category_id = t2.category_id
JOIN (SELECT category_id, SUM(replacement_cost) from category_rev_det WHERE was_returned = 'No'GROUP BY category_id)
      AS t3
ON t1.category_id = t3.category_id
JOIN (SELECT  category.category_id, sum(t1.init_inv)  FROM
	 (SELECT t1.film_id, (t1.count * film.replacement_cost) as init_inv from(
	     select film_id, count(film_id)  from inventory 
	     GROUP BY inventory.film_id)
		 as t1
	JOIN film ON film.film_id = t1.film_id)
     AS t1
     JOIN film ON t1.film_id = film.film_id
     JOIN film_category ON film_category.film_id = film.film_id
     JOIN category ON category.category_id = film_category.category_id
     group by category.category_id)
	 as t4
ON t1.category_id = t4.category_id
ORDER BY category_id;	
RETURN Null;

END;
$$
;




SELECT * from category_rev_sum;
--part F: create stored procedure to refresh data in both summary and detail tables.

CREATE OR REPLACE PROCEDURE rebuild_category_report()
language plpgsql
AS $$ 

BEGIN

	TRUNCATE Table category_rev_det;
	TRUNCATE Table category_rev_sum;
	INSERT INTO category_rev_det
	SELECT category.category_id,
	category.name, 
	film.title,
	was_returned(rental.return_date), 
	film.replacement_cost, 
	payment.amount from payment
	JOIN rental ON rental.rental_id = payment.rental_id
	JOIN inventory ON inventory.inventory_id = rental.inventory_id
	JOIN film ON film.film_id = inventory.film_id
	JOIN film_category ON film_category.film_id = film.film_id
	JOIN category ON category.category_id = film_category.category_id
	ORDER BY category.category_id;
END;
$$
;

--part D: populate the detail table
INSERT INTO category_rev_det
SELECT category.category_id,
category.name, 
film.title,
was_returned(rental.return_date), 
film.replacement_cost, 
payment.amount from payment
JOIN rental ON rental.rental_id = payment.rental_id
JOIN inventory ON inventory.inventory_id = rental.inventory_id
JOIN film ON film.film_id = inventory.film_id
JOIN film_category ON film_category.film_id = film.film_id
JOIN category ON category.category_id = film_category.category_id
ORDER BY category.category_id;

CALL rebuild_category_report();
SELECT * FROM rental;