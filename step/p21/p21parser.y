
%{

package p21

import (
	"fmt"
	"math/big"
)

%}

%union {
	num *big.Rat
	string_value string
	name_value string
}
%token	<num>	NUM

%token <string> STRING

%token <keyword> KEYWORD

%token <binary> BINARY

%token <entity_word> ENTITY_WORD

%token <entity_word> ENTITY_NAME

%token KEYWORD
%token ENTITY_INSTANCE_NAME
%token VALUE_INSTANCE_NAME
%token OCCURRENCE_NAME
%token ANCHOR_NAME
%token TAG_NAME
%token RESOURCE
%token ENUMERATION
%token SIGNATURE_CONTENT /* = BASE64 . */

%token ISO_10303_21
%token END_ISO_10303_21
%token ENDSEC
%token HEADER
%token FILE_DESCRIPTION
%token FILE_NAME
%token FILE_SCHEMA

%token ANCHOR
%token REFERENCE
%token DATA0
%token DATA1
%token SIGNATURE

%start EXCHANGE_FILE

%%
EXCHANGE_FILE      : ISO_10303_21 { fmt.Printf(" =>ISO_10303_21 \n") }
                     HEADER_SECTION { fmt.Printf(" =>HEADER_SECTION \n") }
                     ANCHOR_SECTION { fmt.Printf(" =>ANCHOR_SECTION \n") }
                     REFERENCE_SECTION { fmt.Printf(" =>REFERENCE_SECTION \n") }
                     DATA_SECTION { fmt.Printf(" =>DATA_SECTION \n") }
                     END_ISO_10303_21 { fmt.Printf(" =>END_ISO_10303_21 \n") }
                     SIGNATURE_SECTION { fmt.Printf(" =>SIGNATURE_SECTION \n") }
                     ;
HEADER_SECTION     : HEADER { fmt.Printf(" =>HEADER \n") }
                     FILE_DESCRIPTION_BLOCK { fmt.Printf(" =>FILE_DESCRIPTION_BLOCK \n") }
                     FILE_NAME_BLOCK { fmt.Printf(" =>FILE_NAME_BLOCK \n") }
                     FILE_SCHEMA_BLOCK { fmt.Printf(" =>FILE_SCHEMA_BLOCK \n") }
                     HEADER_ENTITY { fmt.Printf(" =>HEADER_ENTITY \n") }
                     ENDSEC { fmt.Printf(" =>ENDSEC-HEADER \n") }
                     ;
FILE_DESCRIPTION_BLOCK : FILE_DESCRIPTION  { fmt.Printf(" =>FILE_DESCRIPTION \n") }
                     '('      { fmt.Printf(" =>( \n") }
                     PARAMETER_LIST1  { fmt.Printf(" =>PARAMETER_LIST1a \n") }
                     ')'  { fmt.Printf(" =>)1a \n") }
                     ';'  { fmt.Printf(" =>;1a \n") }
                     ;
FILE_NAME_BLOCK : FILE_NAME  { fmt.Printf(" =>FILE_NAME \n") }
                     '('      { fmt.Printf(" =>( \n") }
                     PARAMETER_LIST1  { fmt.Printf(" =>PARAMETER_LIST1b \n") }
                     ')'  { fmt.Printf(" =>)1b \n") }
                     ';'  { fmt.Printf(" =>;1b \n") }
                     ;
FILE_SCHEMA_BLOCK : FILE_SCHEMA { fmt.Printf(" =>FILE_SCHEMA \n") }
                     '('      { fmt.Printf(" =>( \n") }
                     PARAMETER_LIST1  { fmt.Printf(" =>PARAMETER_LIST1c \n") }
                     ')'  { fmt.Printf(" =>)1c \n") }
                     ';'  { fmt.Printf(" =>;1c \n") }
                     ;
HEADER_ENTITY      : /* nothing */ { fmt.Printf(" =>nothing \n") }
                   | KEYWORD
                     '('
                     PARAMETER_LIST1
                     ')'
                     ';'
                     HEADER_ENTITY
                   ;

PARAMETER_LIST0    : /* nothing */           { fmt.Printf(" =>nothing1 \n") }
                   | PARAMETER    { fmt.Printf(" =>PARAMETER1 \n") }
                     NEXT_PARAMETER { fmt.Printf(" =>NEXT_PARAMETER1 \n") }
                   ;
PARAMETER_LIST1    : PARAMETER   { fmt.Printf(" =>PARAMETER2 \n") }
                     NEXT_PARAMETER  { fmt.Printf(" =>NEXT_PARAMETER2 \n") }
                   ;
NEXT_PARAMETER     : /* nothing */   { fmt.Printf(" =>nothing3 \n") }
                   | ','             { fmt.Printf(" =>, \n") }
                     PARAMETER       { fmt.Printf(" =>PARAMETER3 \n") }
                     NEXT_PARAMETER  { fmt.Printf(" =>NEXT_PARAMETER3 \n") }
                   ;
PARAMETER          : TYPED_PARAMETER   { fmt.Printf(" =>TYPED_PARAMETER \n") }
                   | UNTYPED_PARAMETER { fmt.Printf(" =>UNTYPED_PARAMETER \n") }
                   | OMITTED_PARAMETER { fmt.Printf(" =>OMITTED_PARAMETER \n") }
                   ;
TYPED_PARAMETER    : KEYWORD
                     '('
                     PARAMETER
                     ')'
                   ;
UNTYPED_PARAMETER  : '$'              { fmt.Printf(" =>$ \n") }
                   | NUM              { fmt.Printf(" =>NUMBER \n") }
                   | STRING           { fmt.Printf(" =>STRING \n") }
//                   | ENTITY_NAME      { fmt.Printf(" =>ENTITY_NAME \n") } // no used, see SUBSUPER_RECORD (complex instance)
                   | ENUMERATION      { fmt.Printf(" =>ENUMERATION \n") }
//                   | BINARY           { fmt.Printf(" =>BINARY \n") } // treated as enumeration
                   | LIST             { fmt.Printf(" =>LIST \n") }
                   ;
OMITTED_PARAMETER  : '*' ;
LIST               : '(' PARAMETER_LIST0 ')' ;

ANCHOR_SECTION     : /* nothing */
                   | ANCHOR ANCHOR_LIST ENDSEC;
ANCHOR_LIST        : /* nothing */
                   | ANCHOR_NAME '=' ANCHOR_ITEM ANCHOR_TAG ';' ANCHOR ;
ANCHOR_ITEM        : '$' | NUM | STRING | ENUMERATION | BINARY
                     | OCCURRENCE_NAME | RESOURCE | ANCHOR_ITEM_LIST ;
ANCHOR_ITEM_LIST   : '(' ANCHOR_ITEM_LISTX ')' ;
ANCHOR_ITEM_LISTX  : /* nothing */
                   | ANCHOR_ITEM ANCHOR_ITEM_NEXT ;
ANCHOR_ITEM_NEXT   : /* nothing */
                   | ',' ANCHOR_ITEM ANCHOR_ITEM_NEXT ;
ANCHOR_TAG         : /* nothing */
                   | '{' TAG_NAME ':' ANCHOR_ITEM '}' ;

REFERENCE_SECTION  : /* nothing */
                   | REFERENCE REFERENCE_LIST ENDSEC ;
REFERENCE_LIST     : /* nothing */
                   | OCCURRENCE_NAME '=' RESOURCE ';' REFERENCE_LIST;

DATA_SECTION       : DATA_SECTION0           { fmt.Printf(" =>DATA \n") }
                   | DATA_SECTION1           { fmt.Printf(" =>DATA \n") }
                   ;
DATA_SECTION0      : DATA0                   { fmt.Printf(" =>DATA0 \n") }
                     DATA_PARAMETER_LIST     { fmt.Printf(" =>DATA_PARAMETER_LIST \n") }
                     ';'                     { fmt.Printf(" =>;-data \n") }
                     ENTITY_INSTANCE_LIST    { fmt.Printf(" =>ENTITY_INSTANCE_LIST0 \n") }
                     ENDSEC                  { fmt.Printf(" =>ENDSEC-DATA \n") }
                   ;
DATA_SECTION1      : DATA1                   { fmt.Printf(" =>DATA1 \n") }
                     ENTITY_INSTANCE_LIST    { fmt.Printf(" =>ENTITY_INSTANCE_LIST1 \n") }
                     ENDSEC                  { fmt.Printf(" =>ENDSEC-DATA \n") }
                   ;
DATA_PARAMETER_LIST : /* nothing */         { fmt.Printf(" =>nothing-DATA_PARAMETER_LIST \n") }
                     | '('      { fmt.Printf(" =>(-DATA-PARAMETER \n") }
                       PARAMETER_LIST1  { fmt.Printf(" =>PARAMETER_LIST1c \n") }
                       ')'  { fmt.Printf(" =>)-DATA-PARAMETER \n") }
                    ;
ENTITY_INSTANCE_LIST: /* nothing */          { fmt.Printf(" =>nothing-ENTITY_INSTANCE_LIST \n") }
                    | ENTITY_INSTANCE        { fmt.Printf(" =>ENTITY_INSTANCE \n") }
                      ENTITY_INSTANCE_LIST   { fmt.Printf(" =>ENTITY_INSTANCE_LIST \n") }
                    ;
ENTITY_INSTANCE    : ENTITY_INSTANCE_NAME  { fmt.Printf(" =>ENTITY_INSTANCE_NAME \n") }
                     '='  { fmt.Printf(" =>=-ENTITY_INSTANCE \n") }
                     SIMPLE_OR_COMPLEX { fmt.Printf(" =>SIMPLE_OR_COMPLEX \n") }
                     ';'  { fmt.Printf(" =>;-ENTITY_INSTANCE \n") }
                   ;
SIMPLE_OR_COMPLEX  : SIMPLE_RECORD  { fmt.Printf(" =>SIMPLE_ENTITY_INSTANCE \n") }
                   | SUBSUPER_RECORD { fmt.Printf(" =>COMPLEX_ENTITY_INSTANCE \n") }
                   ;
SIMPLE_RECORD      : KEYWORD
                     '('
                     PARAMETER_LIST0
                     ')'
                   ;
SUBSUPER_RECORD    : '('
                     SIMPLE_RECORD
                     SIMPLE_RECORD_LIST0
                     ')'
                    ;
SIMPLE_RECORD_LIST0 : /* nothing */
                    | SIMPLE_RECORD SIMPLE_RECORD_LIST0 ;

SIGNATURE_SECTION  : /* nothing */
                   | SIGNATURE SIGNATURE_CONTENT ENDSEC;


%%
