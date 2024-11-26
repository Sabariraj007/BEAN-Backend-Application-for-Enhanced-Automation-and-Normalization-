create table xxits_ppp_column_t (
    column_name        varchar2(4000),
    data_type          varchar2(4000),
    table_name         varchar2(4000),
    attribute_category varchar2(4000)
);

create or replace procedure xxits_ppp_create_tables_proc as
    l_sql varchar2(4000);
    l_current_table varchar2(50);
    l_columns_added varchar2(4000);  
begin
    for rec in (select table_name, column_name, data_type 
                from xxits_ppp_column_t 
                where table_name in ('header', 'line')
                order by table_name, column_name) 
    loop
        
        if l_current_table is null or l_current_table != rec.table_name then
            if l_sql is not null then
               
                l_sql := l_sql || ')';
                execute immediate l_sql;
            end if;
            
            l_sql := 'CREATE TABLE xxits_ppp_' || rec.table_name || '_t (' || rec.column_name || ' ' || rec.data_type;
            l_columns_added := rec.column_name;  
            l_current_table := rec.table_name;
        else
            if instr(l_columns_added, rec.column_name) = 0 then
                l_sql := l_sql || ', ' || rec.column_name || ' ' || rec.data_type;
                l_columns_added := l_columns_added || ',' || rec.column_name;  
            end if;
        end if;
    end loop;

   
    if l_sql is not null then
        l_sql := l_sql || ')';
        execute immediate l_sql;
    end if;
end;

begin
xxits_ppp_create_tables_proc();
end;

create or replace procedure XXITS_PPP_UPDATE_HEADER_DATA_PROC as
begin
 
  for REC in (select * from XXITS_PPP_COLUMN_T where TABLE_NAME = 'header') loop
    update XXITS_PPP_HEADER_T 
    set ERROR_COLUMN = 'N', 
        ERROR_MSG = null;
  end loop;
end XXITS_PPP_UPDATE_HEADER_DATA_PROC;
/

begin
XXITS_PPP_UPDATE_HEADER_DATA_PROC();
end;

create or replace procedure XXITS_PPP_UPDATE_LINE_DATA_PROC as
begin
  for REC in (select * from XXITS_PPP_COLUMN_T where TABLE_NAME = 'line') loop
    update XXITS_PPP_LINE_T 
    set ERROR_COLUMN = 'N', 
        ERROR_MSG = null;
  end loop;
end XXITS_PPP_UPDATE_LINE_DATA_PROC;
/

begin
 XXITS_PPP_UPDATE_LINE_DATA_PROC();
end;


create or replace procedure xxits_ppp_insert_vrp_data_proc as
    l_sql varchar2(4000);
    l_columns_header varchar2(4000) := '';
    l_columns_line varchar2(4000) := '';
begin
    -- Accumulate column definitions for `header` and `line` tables
    for rec in (select column_name, data_type, table_name from xxits_ppp_column_t order by table_name) loop
        if rec.table_name = 'header' then
            l_columns_header := l_columns_header || rec.column_name || ' ' || rec.data_type || ', ';
        elsif rec.table_name = 'line' then
            l_columns_line := l_columns_line || rec.column_name || ' ' || rec.data_type || ', ';
        end if;
    end loop;

    -- Remove trailing commas and spaces
    l_columns_header := rtrim(l_columns_header, ', ');
    l_columns_line := rtrim(l_columns_line, ', ');

    -- Create validated, rejected, and processing tables for `header` data
    l_sql := 'CREATE TABLE xxits_ppp_validated_header_t (' || l_columns_header || ')';
    execute immediate l_sql;

    l_sql := 'CREATE TABLE xxits_ppp_rejected_header_t (' || l_columns_header || ')';
    execute immediate l_sql;

    l_sql := 'CREATE TABLE xxits_ppp_processing_header_t (' || l_columns_header || ')';
    execute immediate l_sql;

    -- Create validated, rejected, and processing tables for `line` data
    l_sql := 'CREATE TABLE xxits_ppp_validated_line_t (' || l_columns_line || ')';
    execute immediate l_sql;

    l_sql := 'CREATE TABLE xxits_ppp_rejected_line_t (' || l_columns_line || ')';
    execute immediate l_sql;

    l_sql := 'CREATE TABLE xxits_ppp_processing_line_t (' || l_columns_line || ')';
    execute immediate l_sql;
end xxits_ppp_insert_vrp_data_proc;
/

-- Run the procedure
BEGIN
    xxits_ppp_insert_vrp_data_proc();
END;
/




BEGIN
    xxits_ppp_insert_vrp_data_proc();
END;
/

create or replace procedure xxits_ppp_validate_data_proc (
    new_phone in varchar2,
    new_email in varchar2
) as
    duplicate_contact number := 0;
    duplicate_email number := 0;
begin
    for rec in (select f.customer_id, f.customer_name, f.contact_number, f.email, f.location, 
                       f.header_id as header_id_f, g.header_id as header_id_g 
                from xxits_ppp_header_t f, xxits_ppp_line_t g 
                where f.header_id = g.header_id) loop

        if rec.contact_number is null then
            insert into xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            values (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Phone Number Is Invalid.');
        elsif rec.contact_number not like '9%' and rec.contact_number not like '8%' 
              and rec.contact_number not like '7%' and rec.contact_number not like '6%' then
            insert into xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            values (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Phone Number Should Start With 6, 7, 8, Or 9.');
        elsif length(rec.contact_number) != 10 then
            insert into xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            values (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Phone Number Length Should Be Exactly 10 Digits.');
        else
            select count(*) into duplicate_contact
            from xxits_ppp_header_t
            where contact_number = rec.contact_number;
            if duplicate_contact > 1 then
                insert into xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
                values (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Phone Number Is Duplicated.');
            end if;
        end if;

        if rec.email is null then
            insert into xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            values (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Email Is Invalid.');
        elsif rec.email not like '%@gmail.com' then
            insert into xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            values (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Email Must Be In The Format Of @gmail.com.');
        elsif initcap(rec.email) = rec.email then
            insert into xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            values (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Email Should Not Start With Capital Letters.');
        elsif length(rec.email) >= 30 then
            insert into xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            values (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Email Should Not Exceed 30 Characters.');
        else
            select count(*) into duplicate_email
            from xxits_ppp_header_t
            where email = rec.email;

            if duplicate_email > 1 then
                insert into xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
                values (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Duplicate Email Found.');
            end if;
        end if;
    end loop;

    for rec_1 in (select h.customer_id, h.customer_name, h.contact_number, h.email,h.location, i.order_date, i.payment_method, i.payment_status, 
                         h.header_id as header_id_h, i.header_id as header_id_i 
                  from xxits_ppp_header_t h, xxits_ppp_line_t i 
                  where h.header_id = i.header_id) loop

      
        if rec_1.order_date is null or not regexp_like(to_char(rec_1.order_date, 'MM-DD-YYYY'), '^\d{2}-\d{2}-\d{4}$') then
            insert into xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            values (rec_1.header_id_h, rec_1.customer_id, rec_1.customer_name, rec_1.contact_number, rec_1.email, rec_1.location, 'E', 'Order Date Format Should Be MM-DD-YYYY.');
        end if;

      
        if (rec_1.payment_method = 'Cash' and rec_1.payment_status = 'Unpaid') or
           (rec_1.payment_method = 'Paytm' and rec_1.payment_status = 'Unpaid') then
            insert into xxits_ppp_processing_header_t (header_id, customer_id, customer_name, contact_number, email, error_column, location, error_msg)
            values (rec_1.header_id_h, rec_1.customer_id, rec_1.customer_name, rec_1.contact_number, rec_1.email, rec_1.location, 'E', 'Unpaid Payment Detected.');
        else
            insert into xxits_ppp_validated_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            values (rec_1.header_id_h, rec_1.customer_id, rec_1.customer_name, rec_1.contact_number, rec_1.email, rec_1.location, 'V', 'Valid');
        end if;
    end loop;
    
     

    for rec_3 in (select * from xxits_ppp_rejected_header_t) loop
    case
        when rec_3.error_msg = 'Phone Number Is Invalid.' then
            update xxits_ppp_header_t
            set contact_number = new_phone, error_column = 'V', error_msg = 'Valid'
            where header_id = rec_3.header_id;
            insert into xxits_ppp_validated_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            values (rec_3.header_id, rec_3.customer_id, rec_3.customer_name, rec_3.contact_number, rec_3.email, rec_3.location, 'V', 'Valid');
        when rec_3.error_msg = 'Email Is Invalid.' then
            update xxits_ppp_header_t
            set email = new_email, error_column = 'V', error_msg = 'Valid'
            where header_id = rec_3.header_id;
            insert into xxits_ppp_validated_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            values (rec_3.header_id, rec_3.customer_id, rec_3.customer_name, rec_3.contact_number, rec_3.email, rec_3.location, 'V', 'Valid');
        else
            
            null; 
    end case;

   -- DELETE FROM xxits_ppp_rejected_header_t WHERE header_id = rec_3.header_id;
end loop;


    for rec_4 in (select * from xxits_ppp_processing_header_t) loop
        case
            when rec_4.error_msg = 'Unpaid Payment Detected.' then
                update xxits_ppp_line_t
                set payment_status = 'Paid', error_column = 'V', error_msg = 'Valid'
                where header_id = rec_4.header_id;
                insert into xxits_ppp_validated_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
                values (rec_4.header_id, rec_4.customer_id, rec_4.customer_name, rec_4.contact_number, rec_4.email, rec_4.location, 'V', 'Valid');
        end case;

        --DELETE FROM xxits_ppp_processing_header_t WHERE header_id = rec_4.header_id;
    end loop;

    
end xxits_ppp_validate_data_proc;


begin
xxits_ppp_validate_data_proc(6673888772,'bigil@gmail.com');
end;

CREATE OR REPLACE PROCEDURE xxits_ppp_validate_data_proc (
    new_phone IN VARCHAR2,
    new_email IN VARCHAR2
) AS
    duplicate_contact NUMBER := 0;
    duplicate_email NUMBER := 0;
BEGIN
    FOR rec IN (SELECT f.customer_id, f.customer_name, f.contact_number, f.email, f.location, 
                       f.header_id AS header_id_f, g.header_id AS header_id_g 
                FROM xxits_ppp_header_t f, xxits_ppp_line_t g 
                WHERE f.header_id = g.header_id) LOOP

        IF rec.contact_number IS NULL THEN
            INSERT INTO xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            VALUES (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Phone Number Is Invalid.');
        ELSIF rec.contact_number NOT LIKE '9%' AND rec.contact_number NOT LIKE '8%' 
              AND rec.contact_number NOT LIKE '7%' AND rec.contact_number NOT LIKE '6%' THEN
            INSERT INTO xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            VALUES (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Phone Number Should Start With 6, 7, 8, Or 9.');
        ELSIF LENGTH(rec.contact_number) != 10 THEN
            INSERT INTO xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            VALUES (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Phone Number Length Should Be Exactly 10 Digits.');
        ELSE
            SELECT COUNT(*) INTO duplicate_contact
            FROM xxits_ppp_header_t
            WHERE contact_number = rec.contact_number;
            IF duplicate_contact > 1 THEN
                INSERT INTO xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
                VALUES (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Phone Number Is Duplicated.');
            END IF;
        END IF;

        IF rec.email IS NULL THEN
            INSERT INTO xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            VALUES (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Email Is Invalid.');
        ELSIF rec.email NOT LIKE '%@gmail.com' THEN
            INSERT INTO xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            VALUES (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Email Must Be In The Format Of @gmail.com.');
        ELSIF INITCAP(rec.email) = rec.email THEN
            INSERT INTO xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            VALUES (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Email Should Not Start With Capital Letters.');
        ELSIF LENGTH(rec.email) >= 30 THEN
            INSERT INTO xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            VALUES (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Email Should Not Exceed 30 Characters.');
        ELSE
            SELECT COUNT(*) INTO duplicate_email
            FROM xxits_ppp_header_t
            WHERE email = rec.email;

            IF duplicate_email > 1 THEN
                INSERT INTO xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
                VALUES (rec.header_id_f, rec.customer_id, rec.customer_name, rec.contact_number, rec.email, rec.location, 'E', 'Duplicate Email Found.');
            END IF;
        END IF;
    END LOOP;

    FOR rec_1 IN (SELECT h.customer_id, h.customer_name, h.contact_number, h.email,h.location, i.order_date, i.payment_method, i.payment_status, 
                         h.header_id AS header_id_h, i.header_id AS header_id_i 
                  FROM xxits_ppp_header_t h, xxits_ppp_line_t i 
                  WHERE h.header_id = i.header_id) LOOP

      
        IF rec_1.order_date IS NULL OR NOT regexp_like(TO_CHAR(rec_1.order_date, 'MM-DD-YYYY'), '^\d{2}-\d{2}-\d{4}$') THEN
            INSERT INTO xxits_ppp_rejected_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            VALUES (rec_1.header_id_h, rec_1.customer_id, rec_1.customer_name, rec_1.contact_number, rec_1.email, rec_1.location, 'E', 'Order Date Format Should Be MM-DD-YYYY.');
        END IF;

      
        IF (rec_1.payment_method = 'Cash' AND rec_1.payment_status = 'Unpaid') OR
           (rec_1.payment_method = 'Paytm' AND rec_1.payment_status = 'Unpaid') THEN
            INSERT INTO xxits_ppp_processing_header_t (header_id, customer_id, customer_name, contact_number, email, error_column, location, error_msg)
            VALUES (rec_1.header_id_h, rec_1.customer_id, rec_1.customer_name, rec_1.contact_number, rec_1.email, rec_1.location, 'E', 'Unpaid Payment Detected.');
        ELSE
            INSERT INTO xxits_ppp_validated_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            VALUES (rec_1.header_id_h, rec_1.customer_id, rec_1.customer_name, rec_1.contact_number, rec_1.email, rec_1.location, 'V', 'Valid');
        END IF;
    END LOOP;
    
     

    FOR rec_3 IN (SELECT * FROM xxits_ppp_rejected_header_t) LOOP
    CASE
        WHEN rec_3.error_msg = 'Phone Number Is Invalid.' THEN
            UPDATE xxits_ppp_header_t
            SET contact_number = new_phone, error_column = 'V', error_msg = 'Valid'
            WHERE header_id = rec_3.header_id;
            INSERT INTO xxits_ppp_validated_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            VALUES (rec_3.header_id, rec_3.customer_id, rec_3.customer_name, rec_3.contact_number, rec_3.email, rec_3.location, 'V', 'Valid');
        WHEN rec_3.error_msg = 'Email Is Invalid.' THEN
            UPDATE xxits_ppp_header_t
            SET email = new_email, error_column = 'V', error_msg = 'Valid'
            WHERE header_id = rec_3.header_id;
            INSERT INTO xxits_ppp_validated_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
            VALUES (rec_3.header_id, rec_3.customer_id, rec_3.customer_name, rec_3.contact_number, rec_3.email, rec_3.location, 'V', 'Valid');
        ELSE
            
            NULL; 
    END CASE;

   -- DELETE FROM xxits_ppp_rejected_header_t WHERE header_id = rec_3.header_id;
END LOOP;


    FOR rec_4 IN (SELECT * FROM xxits_ppp_processing_header_t) LOOP
        CASE
            WHEN rec_4.error_msg = 'Unpaid Payment Detected.' THEN
                UPDATE xxits_ppp_line_t
                SET payment_status = 'Paid', error_column = 'V', error_msg = 'Valid'
                WHERE header_id = rec_4.header_id;
                INSERT INTO xxits_ppp_validated_header_t (header_id, customer_id, customer_name, contact_number, email, location, error_column, error_msg)
                VALUES (rec_4.header_id, rec_4.customer_id, rec_4.customer_name, rec_4.contact_number, rec_4.email, rec_4.location, 'V', 'Valid');
        END CASE;

        --DELETE FROM xxits_ppp_processing_header_t WHERE header_id = rec_4.header_id;
    END LOOP;

    
END xxits_ppp_validate_data_proc;


Begin
xxits_ppp_validate_data_proc(6673888772,'bigil@gmail.com');
end;

CREATE OR REPLACE PROCEDURE xxits_ppp_validate_line_data_proc (
    new_phone IN VARCHAR2,
    new_email IN VARCHAR2
) AS
    duplicate_contact NUMBER := 0;
    duplicate_email NUMBER := 0;
BEGIN


    
    FOR rec_1 IN (SELECT h.header_id, h.customer_id, h.customer_name, h.contact_number, h.email, h.location, 
                          i.purchase_date, i.line_id, i.vendor_name, i.payment_method, i.total_amount, 
                          i.unit_price, i.gst, i.quantity, i.product_description, i.order_date, 
                          i.product_name, i.category_name, i.product_id, i.cart_id, i.category_id, 
                          i.purchase_id, i.order_status, i.payment_status, i.payment_id, 
                          i.vendor_id, i.reference_number, 
                          h.header_id AS header_id_h, i.header_id AS header_id_i 
                   FROM xxits_ppp_header_t h, xxits_ppp_line_t i 
                   WHERE h.header_id = i.header_id) LOOP

        -- Validate order_date (MM-DD-YYYY format)
        IF rec_1.order_date IS NULL OR NOT regexp_like(TO_CHAR(rec_1.order_date, 'MM-DD-YYYY'), '^\d{2}-\d{2}-\d{4}$') THEN
            INSERT INTO xxits_ppp_rejected_line_t (line_id, vendor_name, payment_method, total_amount, unit_price, gst, 
                                                    quantity, product_description, order_date, product_name, category_name, 
                                                    product_id, cart_id, category_id, purchase_id, order_status, 
                                                    payment_status, payment_id, vendor_id, reference_number, 
                                                    header_id, error_column, error_msg)
            VALUES (rec_1.line_id, rec_1.vendor_name, rec_1.payment_method, rec_1.total_amount, rec_1.unit_price, rec_1.gst,
                    rec_1.quantity, rec_1.product_description, rec_1.order_date, rec_1.product_name, rec_1.category_name, 
                    rec_1.product_id, rec_1.cart_id, rec_1.category_id, rec_1.purchase_id, rec_1.order_status, 
                    rec_1.payment_status, rec_1.payment_id, rec_1.vendor_id, rec_1.reference_number, rec_1.header_id, 
                    'E', 'Order Date Format Should Be MM-DD-YYYY.');
        END IF;

        -- Validate payment_method and payment_status
        IF (rec_1.payment_method = 'Cash' AND rec_1.payment_status = 'Unpaid') OR
           (rec_1.payment_method = 'Paytm' AND rec_1.payment_status = 'Unpaid') THEN
            INSERT INTO xxits_ppp_processing_line_t (line_id, vendor_name, payment_method, total_amount, unit_price, gst, 
                                                     quantity, product_description, order_date, product_name, category_name, 
                                                     product_id, cart_id, category_id, purchase_id, order_status, 
                                                     payment_status, payment_id, vendor_id, reference_number, 
                                                     header_id, error_column, error_msg)
            VALUES (rec_1.line_id, rec_1.vendor_name, rec_1.payment_method, rec_1.total_amount, rec_1.unit_price, rec_1.gst,
                    rec_1.quantity, rec_1.product_description, rec_1.order_date, rec_1.product_name, rec_1.category_name, 
                    rec_1.product_id, rec_1.cart_id, rec_1.category_id, rec_1.purchase_id, rec_1.order_status, 
                    rec_1.payment_status, rec_1.payment_id, rec_1.vendor_id, rec_1.reference_number, rec_1.header_id, 
                    'E', 'Unpaid Payment Detected.');
        ELSE
            INSERT INTO xxits_ppp_validated_line_t (line_id, vendor_name, payment_method, total_amount, unit_price, gst, 
                                                    quantity, product_description, order_date, product_name, category_name, 
                                                    product_id, cart_id, category_id, purchase_id, order_status, 
                                                    payment_status, payment_id, vendor_id, reference_number, 
                                                    header_id, error_column, error_msg)
            VALUES (rec_1.line_id, rec_1.vendor_name, rec_1.payment_method, rec_1.total_amount, rec_1.unit_price, rec_1.gst,
                    rec_1.quantity, rec_1.product_description, rec_1.order_date, rec_1.product_name, rec_1.category_name, 
                    rec_1.product_id, rec_1.cart_id, rec_1.category_id, rec_1.purchase_id, rec_1.order_status, 
                    rec_1.payment_status, rec_1.payment_id, rec_1.vendor_id, rec_1.reference_number, rec_1.header_id, 
                    'V', 'Valid');
        END IF;
    END LOOP;

    -- Revalidate and move rejected line data to validated line
    FOR rec_3 IN (SELECT * FROM xxits_ppp_rejected_line_t) LOOP
        BEGIN
            CASE
                WHEN rec_3.error_msg = 'Order Date Format Should Be MM-DD-YYYY.' THEN
                    UPDATE xxits_ppp_line_t
                    SET order_date = TO_DATE(new_phone, 'MM-DD-YYYY'), error_column = 'V', error_msg = 'Valid'
                    WHERE line_id = rec_3.line_id;
                    INSERT INTO xxits_ppp_validated_line_t (line_id, vendor_name, payment_method, total_amount, unit_price, gst, 
                                                            quantity, product_description, order_date, product_name, category_name, 
                                                            product_id, cart_id, category_id, purchase_id, order_status, 
                                                            payment_status, payment_id, vendor_id, reference_number, 
                                                            header_id, error_column, error_msg)
                    VALUES (rec_3.line_id, rec_3.vendor_name, rec_3.payment_method, rec_3.total_amount, rec_3.unit_price, rec_3.gst,
                            rec_3.quantity, rec_3.product_description, rec_3.order_date, rec_3.product_name, rec_3.category_name, 
                            rec_3.product_id, rec_3.cart_id, rec_3.category_id, rec_3.purchase_id, rec_3.order_status, 
                            rec_3.payment_status, rec_3.payment_id, rec_3.vendor_id, rec_3.reference_number, rec_3.header_id, 
                            'V', 'Valid');
                ELSE
                    NULL;
            END CASE;
        END;

        -- Optionally, delete rejected line data if you no longer need it
        -- DELETE FROM xxits_ppp_rejected_line_t WHERE line_id = rec_3.line_id;
    END LOOP;

    -- Revalidate and move processing line data to validated line
    FOR rec_4 IN (SELECT * FROM xxits_ppp_processing_line_t) LOOP
        BEGIN
            CASE
                WHEN rec_4.error_msg = 'Unpaid Payment Detected.' THEN
                    UPDATE xxits_ppp_line_t
                    SET payment_status = 'Paid', error_column = 'V', error_msg = 'Valid'
                    WHERE line_id = rec_4.line_id;
                    INSERT INTO xxits_ppp_validated_line_t (line_id, vendor_name, payment_method, total_amount, unit_price, gst, 
                                                            quantity, product_description, order_date, product_name, category_name, 
                                                            product_id, cart_id, category_id, purchase_id, order_status, 
                                                            payment_status, payment_id, vendor_id, reference_number, 
                                                            header_id, error_column, error_msg)
                    VALUES (rec_4.line_id, rec_4.vendor_name, rec_4.payment_method, rec_4.total_amount, rec_4.unit_price, rec_4.gst,
                            rec_4.quantity, rec_4.product_description, rec_4.order_date, rec_4.product_name, rec_4.category_name, 
                            rec_4.product_id, rec_4.cart_id, rec_4.category_id, rec_4.purchase_id, rec_4.order_status, 
                            rec_4.payment_status, rec_4.payment_id, rec_4.vendor_id, rec_4.reference_number, rec_4.header_id, 
                            'V', 'Valid');
                ELSE
                    NULL;
            END CASE;
        END;

        -- Optionally, delete processing line data if you no longer need it
        -- DELETE FROM xxits_ppp_processing_line_t WHERE line_id = rec_4.line_id;
    END LOOP;

END xxits_ppp_validate_line_data_proc;

-- Call the procedure
BEGIN
    xxits_ppp_validate_line_data_proc(6939939220, 'dhanush@gmail.com');
END;

create or replace procedure xxits_ppp_masters_t
is
    cursor master_insert_cursor is
        select 
            c.header_id as c_header_id,
            c.customer_id,
            c.customer_name,
            c.email,
            c.contact_number,
            c.location,
            d.header_id as d_header_id,
            d.line_id,
            d.purchase_date,
            d.quantity,
            d.purchase_id,
            d.payment_id,
            d.payment_method,
            d.payment_status,
            d.cart_id,
            d.category_id,
            d.category_name,
            d.order_date,
            d.order_status,
            d.vendor_id,
            d.vendor_name,
            d.product_id,
            d.product_name,
            d.product_description,
            d.total_amount
        from 
            xxits_ppp_validated_header_t c  ,
            xxits_ppp_validated_line_t d
            where c.header_id = d.header_id;

begin
    execute immediate 'CREATE TABLE Xxits_Ppp_Customer_Master_T (' ||
                      'Header_Id NUMBER, ' ||
                      'Customer_Id NUMBER, ' ||
                      'Customer_Name VARCHAR2(400), ' ||
                      'Location VARCHAR2(400), ' ||
                      'Contact_Number NUMBER, ' ||
                      'Email VARCHAR2(400))';

    execute immediate 'CREATE TABLE Xxits_Ppp_Purchase_Master_T (' ||
                      'Purchase_Id NUMBER, ' ||
                      'Purchase_Date DATE, ' ||
                      'Total_Amount NUMBER)';

    execute immediate 'CREATE TABLE Xxits_Ppp_Product_Master_T (' ||
                      'Product_Id NUMBER, ' ||
                      'Product_Name VARCHAR2(240), ' ||
                      'Product_Description VARCHAR2(240))';

    execute immediate 'CREATE TABLE Xxits_Ppp_Vendor_Master_T (' ||
                      'Vendor_Id NUMBER, ' ||
                      'Vendor_Name VARCHAR2(240), ' ||
                      'Product_Description VARCHAR2(240))';

    execute immediate 'CREATE TABLE Xxits_Ppp_Payment_Master_T (' ||
                      'Payment_Id NUMBER, ' ||
                      'Payment_Method VARCHAR2(240), ' ||
                      'Payment_Status VARCHAR2(240))';

    execute immediate 'CREATE TABLE Xxits_Ppp_Category_Master_T (' ||
                      'Category_Id NUMBER, ' ||
                      'Category_Name VARCHAR2(240))';

    execute immediate 'CREATE TABLE Xxits_Ppp_Cart_Master_T (' ||
                      'Cart_Id NUMBER, ' ||
                      'Product_Id NUMBER, ' ||
                      'Quantity NUMBER)';

    for rec in master_insert_cursor loop
        execute immediate 'INSERT INTO Xxits_Ppp_Customer_Master_T (Header_Id, Customer_Id, Customer_Name, Location, Contact_Number, Email) ' ||
                          'VALUES (:1, :2, :3, :4, :5, :6)'
        using rec.c_header_id, rec.customer_id, rec.customer_name, rec.location, rec.contact_number, rec.email;

        execute immediate 'INSERT INTO Xxits_Ppp_Purchase_Master_T (Purchase_Id, Purchase_Date, Total_Amount) ' ||
                          'VALUES (:1, :2, :3)'
        using rec.purchase_id, rec.purchase_date, rec.total_amount;

        
        execute immediate 'INSERT INTO Xxits_Ppp_Product_Master_T (Product_Id, Product_Name, Product_Description) ' ||
                          'VALUES (:1, :2, :3)'
        using rec.product_id, rec.product_name, rec.product_description;

        
        execute immediate 'INSERT INTO Xxits_Ppp_Vendor_Master_T (Vendor_Id, Vendor_Name, Product_Description) ' ||
                          'VALUES (:1, :2, :3)'
        using rec.vendor_id, rec.vendor_name, rec.product_description;

        
        execute immediate 'INSERT INTO Xxits_Ppp_Payment_Master_T (Payment_Id, Payment_Method, Payment_Status) ' ||
                          'VALUES (:1, :2, :3)'
        using rec.payment_id, rec.payment_method, rec.payment_status;

        
        execute immediate 'INSERT INTO Xxits_Ppp_Category_Master_T (Category_Id, Category_Name) ' ||
                          'VALUES (:1, :2)'
        using rec.category_id, rec.category_name;

        
        execute immediate 'INSERT INTO Xxits_Ppp_Cart_Master_T (Cart_Id, Product_Id, Quantity) ' ||
                          'VALUES (:1, :2, :3)'
        using rec.cart_id, rec.product_id, rec.quantity;
    end loop;
end xxits_ppp_masters_t;

begin
xxits_ppp_masters_t();
end;


CREATE TABLE po_headers_all (
    header_id         NUMBER ,
    customer_id       NUMBER,
    customer_name     VARCHAR2(100),
    contact_number    VARCHAR2(15),
    email             VARCHAR2(100),
    loc               VARCHAR2(100)
);

CREATE TABLE po_lines_all (
    line_id             NUMBER ,
    header_id           NUMBER,
    total_amount        NUMBER,
    attribute_catagory  VARCHAR2(50),
    attribute_1         VARCHAR2(50),
    attribute_2         VARCHAR2(50),
    attribute_3         VARCHAR2(50),
    attribute_4         VARCHAR2(50),
    attribute_5         VARCHAR2(50),
    attribute_6         VARCHAR2(50),
    attribute_7         VARCHAR2(50),
    attribute_8         VARCHAR2(50),
    attribute_9         VARCHAR2(50),
    attribute_10        VARCHAR2(50),
    attribute_11        VARCHAR2(50),
    creation_by         VARCHAR2(50),
    created_date        DATE DEFAULT SYSDATE,
    login_date          DATE,
    login_by            VARCHAR2(50),
);






create or replace procedure xxits_process_to_standard_po_tables as
begin
    insert into po_headers_all (header_id, customer_id, customer_name, contact_number, email, loc)
    select h.header_id, h.customer_id, h.customer_name, h.contact_number, h.email, h.location
    from xxits_ppp_validated_header_t h
    where not exists (
        select 1 from po_headers_all ph where ph.header_id = h.header_id
    );

    insert into po_lines_all (
        line_id, header_id, total_amount, attribute_catagory, attribute_1,
        attribute_2, attribute_3, attribute_4, attribute_5, attribute_6,
        attribute_7, attribute_8, attribute_9, attribute_10, attribute_11,
        creation_by, created_date, login_date, login_by
    )
    select
        l.line_id, l.header_id, l.total_amount, l.attribute_category, 
        l.attribute_1, l.attribute_2, l.attribute_3, l.attribute_4, 
        l.attribute_5, l.attribute_6, l.attribute_7, l.attribute_8, 
        l.attribute_9, l.attribute_10, l.attribute_11, 
        l.creation_by, coalesce(l.created_date, sysdate), 
        coalesce(l.login_date, sysdate), l.login_by
    from xxits_ppp_validated_line_t l
    where not exists (
        select 1 from po_lines_all pl where pl.line_id = l.line_id
    );

    commit;
end xxits_process_to_standard_po_tables;



We need to build one backend application(BEAN):
------------------------------------------------

Requirements:
-------------
1. Create a flat file(XLSX) and insert header and line data
2. Create a header , line , validated and rejected tables dynamically using master table with the help of columns in PLSQL
3. Insert data using import option in SQL Developer
4. Write a package procedure to validate the data and mark error_flag(N , V , P , E) and error_msg column.
5. Validated records should insert into validated table automatically.
6. Rejected records should insert into rejected table automatically.
7. Write an another procedure to process the data into standard table(PO_HEADERS_ALL and PO_LINES_ALL).
8. Create a PLSQL Report by showing master detail report along with dashboards in your creative way.
9. Write an another procedure with parameter(p_type --> new , validated , rejected , processed) to download the entire report XML file into excel file and store it into local disk C Drive under your folder with the name format of "ITS_{P_TYPE(1st Letter)}_BEAN_MMRR_000SeqValue".
10. create an another procedure to move all the files to recycle bin.

Validation:
-----------
1. Duplicate validation must be there as one function
2. Email Validation must be there
3. Expiry days calculation should be there based on expiry date
4. Who columns has to be fetched using special functions
a.Create user table and mention username and userid and get it.
b.Create function to get today's date
5. Triggers has to be worked properly in all the cases.
6. When i delete any records,that has to store in deleted table and script shoule be dynamic(No predifined DDL)

Note:
-----
1. All the standard naming conventions should be followed.
2. Proper project timelines should be submitted to PM before the development.
3. Folder and Files should be stored proper as per ITS standard. 
