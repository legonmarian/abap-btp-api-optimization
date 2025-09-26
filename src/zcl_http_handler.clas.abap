CLASS zcl_http_handler DEFINITION
  PUBLIC
  CREATE PUBLIC .

  PUBLIC SECTION.

    INTERFACES if_http_service_extension .
    INTERFACES if_oo_adt_classrun .

    TYPES: BEGIN OF ty_data_subset,
             column1  TYPE ztarget_table-column1,
             column2  TYPE ztarget_table-column2,
             column3  TYPE ztarget_table-column3,
             column4  TYPE ztarget_table-column4,
             column5  TYPE ztarget_table-column5,
             column6  TYPE ztarget_table-column6,
             column7  TYPE ztarget_table-column7,
             column8  TYPE ztarget_table-column8,
             column9  TYPE ztarget_table-column9,
             column10 TYPE ztarget_table-column10,
             column11 TYPE ztarget_table-column11,
             column12 TYPE ztarget_table-column12,
           END OF ty_data_subset.

    TYPES tt_data_subset TYPE STANDARD TABLE OF ty_data_subset WITH EMPTY KEY.

    CLASS-METHODS get_json
      IMPORTING
        VALUE(itab) TYPE tt_data_subset
      RETURNING
        VALUE(json) TYPE string.

    CLASS-METHODS get_csv
      IMPORTING
        VALUE(itab) TYPE tt_data_subset
      RETURNING
        VALUE(csv)  TYPE string.

  PROTECTED SECTION.
  PRIVATE SECTION.
    METHODS compare_json_vs_csv.
    METHODS compare_json IMPORTING out TYPE REF TO if_oo_adt_classrun_out.

    METHODS convert_json_ui2 IMPORTING data TYPE tt_data_subset RETURNING VALUE(string) TYPE string.
    METHODS convert_json_xco IMPORTING data TYPE tt_data_subset RETURNING VALUE(string) TYPE string.
    METHODS convert_json_transformation IMPORTING data TYPE tt_data_subset RETURNING VALUE(string) TYPE xstring.

    METHODS convert_csv IMPORTING data TYPE tt_data_subset RETURNING VALUE(string) TYPE string.
    METHODS convert_csv_static IMPORTING data TYPE tt_data_subset RETURNING VALUE(string) TYPE string.

    METHODS gzip_csv_multiple_pages IMPORTING page_size TYPE i DEFAULT 1000 max_pages TYPE i DEFAULT 1 CHANGING response TYPE REF TO if_web_http_response request TYPE REF TO if_web_http_request.
    METHODS gzip_csv_single_page    IMPORTING page_size TYPE i DEFAULT 1000 CHANGING response TYPE REF TO if_web_http_response request TYPE REF TO if_web_http_request.
    METHODS gzip_json_single_page   IMPORTING page_size TYPE i DEFAULT 1000 CHANGING response TYPE REF TO if_web_http_response request TYPE REF TO if_web_http_request.

ENDCLASS.



CLASS zcl_http_handler IMPLEMENTATION.


  METHOD compare_json.
    SELECT column1,
         column2,
         column3,
         column4,
         column5,
         column6,
         column7,
         column8,
         column9,
         column10,
         column11,
         column12
    FROM ztarget_table
    INTO TABLE @DATA(page)
    UP TO 1000000 ROWS.

    convert_json_ui2( page ).

*    convert_json_xco( page ).

    DATA(json) = convert_json_transformation( page ).
    out->write( |JSON: { json }| ).
  ENDMETHOD.


  METHOD compare_json_vs_csv.
    SELECT column1,
         column2,
         column3,
         column4,
         column5,
         column6,
         column7,
         column8,
         column9,
         column10,
         column11,
         column12
    FROM ztarget_table
    INTO TABLE @DATA(page)
    UP TO 1000000 ROWS.

    convert_json_transformation( page ).
    convert_csv( page ).
  ENDMETHOD.


  METHOD convert_csv.
    DATA lt_column_names TYPE STANDARD TABLE OF string WITH EMPTY KEY.

    DATA(lo_tab_descr) = CAST cl_abap_tabledescr( cl_abap_tabledescr=>describe_by_data_ref( REF #( data ) ) ).
    DATA(lo_line_descr) = CAST cl_abap_structdescr( lo_tab_descr->get_table_line_type( ) ).

    LOOP AT lo_line_descr->components ASSIGNING FIELD-SYMBOL(<comp>).
      APPEND <comp>-name TO lt_column_names.
      string = string && <comp>-name && ';'.
    ENDLOOP.
    string = string && '\n'.

    LOOP AT data INTO DATA(line).
      LOOP AT lt_column_names INTO DATA(column).
        ASSIGN COMPONENT column OF STRUCTURE line TO FIELD-SYMBOL(<val>).
        string = string && |{ <val> };|.
      ENDLOOP.
      string = string && '\n'.
    ENDLOOP.
  ENDMETHOD.


  METHOD convert_csv_static.
    string = |column_1;column_2;column_3;column_4;column_5;column_6;|
    & |column_7;column_8;column_9;|
    & |column_10;column_11;column_12\n|.

    LOOP AT data INTO DATA(line).
      string = string &&
        |{ line-column1 };{ line-column2 };{ line-column3 };|
      && |{ line-column4 };{ line-column5 };{ line-column6 };|
      && |{ line-column7 };{ line-column8 };{ line-column9 };|
      && |{ line-column10 };{ line-column11 };{ line-column12 }\n|.
    ENDLOOP.
  ENDMETHOD.


  METHOD convert_json_transformation.
    DATA(lo_writer) = cl_sxml_string_writer=>create(
      type = if_sxml=>co_xt_json
    ).

    CALL TRANSFORMATION id
      SOURCE itab   = data
      RESULT XML   lo_writer.

    string = lo_writer->get_output( ).
  ENDMETHOD.


  METHOD convert_json_ui2.
    RETURN /ui2/cl_json=>serialize(
      EXPORTING
        data        = data
    ).
  ENDMETHOD.


  METHOD convert_json_xco.
    RETURN xco_cp_json=>data->from_abap( data )->to_string( ).
  ENDMETHOD.


  METHOD get_csv.
    " Manually build a CSV string from the internal table"
    DATA:
      lt_csv_lines TYPE STANDARD TABLE OF string WITH EMPTY KEY,
      lv_line      TYPE string.
    FIELD-SYMBOLS <fs_row> TYPE ty_data_subset.

    " Header row"
    lv_line = |column1,column2,column3,column4,column5,column6,SiBaseline,SiPromotion,SiLaunch,CsBaseline,CsPromotion,CsLaunch,CsTotal,SiTotal|.
    APPEND lv_line TO lt_csv_lines.

    LOOP AT itab ASSIGNING <fs_row>.
      lv_line = |{ <fs_row>-column1 },{ <fs_row>-column2 },{ <fs_row>-column3 },{ <fs_row>-column4 },|.
      lv_line = |{ lv_line }{ <fs_row>-column5 },{ <fs_row>-column6 },{ <fs_row>-column7 },{ <fs_row>-column8 },|.
      lv_line = |{ lv_line }{ <fs_row>-column9 },{ <fs_row>-column10 },{ <fs_row>-column11 },{ <fs_row>-column12 }|.
      APPEND lv_line TO lt_csv_lines.
    ENDLOOP.

    " Combine all lines into one CSV string with newline separators"
    LOOP AT lt_csv_lines INTO DATA(lv_row).
      CONCATENATE csv lv_row cl_abap_char_utilities=>newline INTO csv.
    ENDLOOP.
  ENDMETHOD.


  METHOD get_json.
    " Serialize the internal table to JSON using ABAP Cloud-compatible class"
    RETURN /ui2/cl_json=>serialize(
      EXPORTING
        data        = itab
        pretty_name = abap_true
    ).
  ENDMETHOD.


  METHOD gzip_csv_multiple_pages.

    CONSTANTS file_name  TYPE string VALUE 'data_subset.csv.gz'.

    "------------------------------------------------------------
    " HTTP response headers
    "------------------------------------------------------------
    response->set_status( 200 ).
    response->set_content_type( 'application/gzip' ).
    response->set_header_field(
        i_name  = 'Content-Disposition'
        i_value = |attachment; filename="{ file_name }"| ).
    response->set_compression(
        options = if_web_http_response=>co_compress_none ).

    "    we cannot use this content encoding because the client will decmpress only the first chunk
*    response->set_header_field(
*        i_name  = 'Content-Encoding'
*        i_value = |deflate| ).


    "------------------------------------------------------------
    " helpers
    "------------------------------------------------------------
    DATA gzip_payload     TYPE xstring.
    DATA gzip_chunk       TYPE xstring.
    DATA csv_buffer       TYPE string.
    DATA page             TYPE STANDARD TABLE OF ztarget_table.
    DATA page_counter     TYPE i VALUE 0.

    "------------------------------------------------------------
    " CSV header  (always one GZIP member)
    "------------------------------------------------------------
    csv_buffer = |column1;column2;column3;column4;column5;column6;|
               & |column7;column8;column9;|
               & |column10;column11;column12\n|.

    cl_abap_gzip=>compress_text(
        EXPORTING text_in  = csv_buffer
        IMPORTING gzip_out = gzip_chunk ).

    CONCATENATE gzip_payload gzip_chunk INTO gzip_payload IN BYTE MODE.

    "------------------------------------------------------------
    " main loop – keep going until we created max_pages
    "------------------------------------------------------------
    WHILE page_counter < max_pages.

      SELECT client,
             column1,
             column2,
             column3,
             column4,
             column5,
             column6,
             column7,
             column8,
             column9,
             column10,
             column11,
             column12
        FROM ztarget_table
        ORDER BY column2
        INTO TABLE @page
        UP TO @page_size ROWS.


      "----------------------------------------------------------
      " build CSV for this page
      "----------------------------------------------------------
      CLEAR csv_buffer.
      LOOP AT page INTO DATA(line).
        csv_buffer = csv_buffer &&
          |{ line-column1 };{ line-column2 };{ line-column3 };|
        && |{ line-column4 };{ line-column5 };{ line-column6 };|
        && |{ line-column7 };{ line-column8 };{ line-column9 };|
        && |{ line-column10 };{ line-column11 };{ line-column12 }\n|.
      ENDLOOP.


      "----------------------------------------------------------
      " compress & append
      "----------------------------------------------------------
      cl_abap_gzip=>compress_text(
          EXPORTING text_in  = csv_buffer
          IMPORTING gzip_out = gzip_chunk ).

      CONCATENATE gzip_payload gzip_chunk
             INTO gzip_payload IN BYTE MODE.

      "----------------------------------------------------------
      " advance cursor & counter
      "----------------------------------------------------------
      CLEAR page.

      page_counter = page_counter + 1.

    ENDWHILE.

    "------------------------------------------------------------
    " send the response
    "------------------------------------------------------------
    response->set_binary( gzip_payload ).

  ENDMETHOD.


  METHOD gzip_csv_single_page.

    CONSTANTS file_name  TYPE string VALUE 'data_subset.csv.gz'.

    "------------------------------------------------------------
    " HTTP response headers
    "------------------------------------------------------------
    response->set_status( 200 ).
    response->set_content_type( 'application/gzip' ).
    response->set_header_field(
        i_name  = 'Content-Disposition'
        i_value = |attachment; filename="{ file_name }"| ).
    response->set_compression(
        options = if_web_http_response=>co_compress_none ).

    response->set_header_field(
        i_name  = 'Content-Encoding'
        i_value = |deflate| ).

    "------------------------------------------------------------
    " helpers
    "------------------------------------------------------------
    DATA csv_buffer       TYPE string.

    "------------------------------------------------------------
    " CSV header
    "------------------------------------------------------------
    csv_buffer = |column1;column2;column3;column4;column5;column6;|
               & |column7;column8;column9;|
               & |column10;column11;column12\n|.


    "------------------------------------------------------------
    " data fetching
    "------------------------------------------------------------
    SELECT column1,
           column2,
           column3,
           column4,
           column5,
           column6,
           column7,
           column8,
           column9,
           column10,
           column11,
           column12
      FROM ztarget_table
      ORDER BY column2
      INTO TABLE @DATA(page)
      UP TO @page_size ROWS.


    "----------------------------------------------------------
    " build CSV for this page
    "----------------------------------------------------------
    LOOP AT page INTO DATA(line).
      csv_buffer = csv_buffer &&
        |{ line-column1 };{ line-column2 };{ line-column3 };|
      && |{ line-column4 };{ line-column5 };{ line-column6 };|
      && |{ line-column7 };{ line-column8 };{ line-column9 };|
      && |{ line-column10 };{ line-column11 };{ line-column12 }\n|.
    ENDLOOP.


    "----------------------------------------------------------
    " compress & append
    "----------------------------------------------------------
    cl_abap_gzip=>compress_text(
        EXPORTING text_in  = csv_buffer
        IMPORTING gzip_out = DATA(gzip) ).


    "------------------------------------------------------------
    " send the response
    "------------------------------------------------------------
    response->set_binary( gzip ).

  ENDMETHOD.


  METHOD gzip_json_single_page.

    CONSTANTS file_name  TYPE string VALUE 'data_subset.csv.gz'.

    "------------------------------------------------------------
    " HTTP response headers
    "------------------------------------------------------------
    response->set_status( 200 ).
    response->set_content_type( 'application/gzip' ).
    response->set_header_field(
        i_name  = 'Content-Disposition'
        i_value = |attachment; filename="{ file_name }"| ).
    response->set_compression(
        options = if_web_http_response=>co_compress_none ).

    response->set_header_field(
        i_name  = 'Content-Encoding'
        i_value = |deflate| ).

    "------------------------------------------------------------
    " data fetching
    "------------------------------------------------------------
    SELECT column1,
           column2,
           column3,
           column4,
           column5,
           column6,
           column7,
           column8,
           column9,
           column10,
           column11,
           column12
      FROM ztarget_table
      ORDER BY column2
      INTO TABLE @DATA(page)
      UP TO @page_size ROWS.


    "----------------------------------------------------------
    " compress & append
    "----------------------------------------------------------
    cl_abap_gzip=>compress_binary(
        EXPORTING raw_in  = convert_json_transformation( page )
        IMPORTING gzip_out = DATA(gzip) ).

    "------------------------------------------------------------
    " send the response
    "------------------------------------------------------------
    response->set_binary( gzip ).

  ENDMETHOD.


  METHOD if_http_service_extension~handle_request.
    " choose one of these
    "gzip_json_single_page( CHANGING request = request response = response ).
    " only for demonstration
    "gzip_csv_single_page( CHANGING request = request response = response ).
    " only for demonstration
    gzip_csv_multiple_pages( CHANGING request = request response = response ).
  ENDMETHOD.


  METHOD if_oo_adt_classrun~main.

*    out->write( |Running ZCL_HTTP_HANDLER| ).
    compare_json_vs_csv( ).
*    compare_json( out = out ).
*    out->write( |Done.| ).
*
*    SELECT column1,
*         column2,
*         column3,
*         column4,
*         column5,
*         column6,
*         column7,
*         column8,
*         column9,
*         column10,
*         column11,
*         column12
*    FROM ztarget_table
*    INTO TABLE @DATA(page)
*    UP TO 1000000 ROWS.
*
*    convert_csv( page ).
*    convert_csv_static( page ).

  ENDMETHOD.
ENDCLASS.
