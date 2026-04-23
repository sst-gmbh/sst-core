%{
package p21

type (
  text = string
  iVal = int
  rVal = float64
  lVal = bool
  list = struct{}
  instance = struct{}
  entref struct{
    i instance
  }
)
%}

%start ExchangeFile

%union {
	text text;
	iVal iVal;
	rVal rVal;
	lVal *lVal;
	// Linked_List list;
  list list;
	// Instance instance;
  instance instance;
  entref entref;
	// struct {
	// 	Instance i;
	// 	Symbol *sym;
	// } entref;
}

%token <text>	ENTITY_NAME
		ENUM
		KEYWORD
		STRING			BINARY
		USER_DEFINED_KEYWORD

%token <iVal>	INTEGER

%token <lVal>	LOGICAL

%token <rVal>	REAL

%token
		COMMA			DATA		ENDSCOPE
		ENDSEC			EQUALS
		END_ISO_10303_21
		HEADER
		LEFT_PAREN
		MISSING			REDEFINE	RIGHT_PAREN
		SCOPE			SEMICOLON	SLASH
		ISO_10303_21

%type <text>	EntityName

%type <entref>	EntityReference

%type <instance>	Simple_Record			ListEntry
		Parameter
		EmbeddedList		List
		Subsuper_Record_List_Element
		Subsuper_Record
		Entity_Instance_RHS

%type <list>	Parameters		Subsuper_Record_List

%% /* beginning of rules section */

ExchangeFile	: {
  // listCounters = STACKcreate();
	// 		scope = scopes;
	// 		scope->dict = product_model->dict;
		  }
		  ISO_10303_21 SEMICOLON
		  HeaderSection
		  DataSection
		  END_ISO_10303_21 SEMICOLON
		;

HeaderSection	: HEADER SEMICOLON HeaderEntities ENDSEC SEMICOLON
		;

HeaderEntities	: ReqHdrEntities
		| ReqHdrEntities OptHdrEntities
		;

ReqHdrEntities	:
		  {
        // expect_entity = "file_description";
      }	HeaderEntity
		  {
        // expect_entity = "file_name";
      }	HeaderEntity
		  {
        // expect_entity = "file_schema";
      }	HeaderEntity
		  {
        // expect_entity = 0;
      }
		;

OptHdrEntities	: OptHdrEntity SEMICOLON
		| OptHdrEntities OptHdrEntity SEMICOLON
		;

OptHdrEntity	: HeaderEntity
		;

DataSection	: {
        // section = "data";
      }
		  DATA SEMICOLON DataEntities ENDSEC SEMICOLON
		;

DataEntities	: DataEntity
		| DataEntities DataEntity

DataEntity	: EntityName EQUALS
		  { /*sprintf(current_entity, $1);*/ }
		  Entity_Instance_RHS SEMICOLON
		  {
        // store_instance($1, $4);
      }
		| EntityName EQUALS SCOPE
		  {
        // PUSH_SCOPE
      }
        DataEntities ENDSCOPE Entity_export
        { /*sprintf(current_entity, $1); */}
        Entity_Instance_RHS SEMICOLON
        {
          // POP_SCOPE
          // store_instance($1, $9);
        }
		;

EntityName	: ENTITY_NAME
		  { $$ = $1; }
		;

EntityReference	: ENTITY_NAME
		  {
        // struct scope *s = scope;
		    $$.i = struct{}{}
		  //   for (;s >= scopes;s--) {
			// if (0 != ($$.i = (Instance)DICTlookup_symbol(s->dict,$1,&$$.sym))) break;
		  //   }
		  //   if ($$.i == 0) {
			//     $$.i = INSTcreate_entity_forward_ref(yylineno,$1);
		  //   }
		  }
		;

HeaderEntity	: KEYWORD LEFT_PAREN Parameters RIGHT_PAREN SEMICOLON
		  {
      //    Entity entity;
		  //   if (expect_entity && !streq(expect_entity,$1)) {
			// ERRORreport_with_line(ERROR_unexpected_header_entity,
			// 	yylineno,expect_entity,$1);
		  //   }
		  //   entity = (Entity)SCOPEfind(header_schema,$1,SCOPE_FIND_ENTITY);
		  //   if (entity == 0) {
			// ERRORreport_with_line(ERROR_unknown_entity,yylineno,$1);
		  //   } else {
			// Instance i;
			// errc = 0;
			// i = INSTcreate_entity(entity, $3, false);
			// if (errc) {
			// 	ERRORreport_with_line(errc,yylineno,$1);
			// } else {
			// 	/* we have to store this somewhere, but */
			// 	/* these are not named.  To avoid stepping */
			// 	/* on user's namespace (which is just */
			// 	/* numbers), use the entity name. */
			// 	store_instance(ENTITYget_name(entity),i);
			// }
		  //   }
      }
		;

/* right-hand-side minus the Scope, that is */
Entity_Instance_RHS	: Simple_Record
			| Subsuper_Record
			;

Subsuper_Record		: LEFT_PAREN Subsuper_Record_List RIGHT_PAREN
			  {
			   $$ = $2//INSTcreate_external($2);
			  }
			;

Subsuper_Record_List	: Subsuper_Record_List_Element
			  { $$ = list{} //LISTcreate();
			  //   if (errc) ERRORreport_with_line(errc,yylineno,"INSTcreate");
			  //   else {
				// LISTadd($$,(Generic)$1);
			  //   }
		          }
			| Subsuper_Record_List Subsuper_Record_List_Element {
			  // if (!errc) {
				// LISTadd($1,(Generic)$2);
			  // }
			}
			;

/* a lot like simple_record, but entities are created with only explicit */
/* attributes */

Subsuper_Record_List_Element	: KEYWORD LEFT_PAREN Parameters RIGHT_PAREN
		  {
      //    Entity entity;
		  //   if (0 == (entity = (Entity)DICTlookup(entity_dict,$1))) {
			// ERRORreport_with_line(ERROR_unknown_entity,yylineno,$1);
			// $$ = 0;
		  //   } else {
			// Instance i;
			// errc = 0;
			// i = INSTcreate_entity(entity, $3, true);
			// i->symbol.line = yylineno;
			// $$ = i;	/* eh? */
			// if (errc) ERRORreport_with_line(errc,yylineno,$1);
		  //   }
        }
		;

Simple_Record	: KEYWORD LEFT_PAREN Parameters RIGHT_PAREN
		  {
      //   Entity entity;
		  //   if (0 == (entity = (Entity)DICTlookup(entity_dict,$1))) {
			// ERRORreport_with_line(ERROR_unknown_entity,yylineno,$1);
			// $$ = 0;
		  //   } else {
			// Instance i;
			// errc = 0;
			// i = INSTcreate_entity(entity, $3, false);
			// i->symbol.line = yylineno;
			// $$ = i;	/* eh? */
			// if (errc) ERRORreport_with_line(errc,yylineno,$1);
		  //   }
      }
		| USER_DEFINED_KEYWORD LEFT_PAREN Parameters RIGHT_PAREN
		  {
        // Instance i = INSTcreate(Type_User_Defined);
		    // i->user_data = (Generic)$3;
		    // $$ = i;
	    }
		;

Parameters	: /* null */
		  {
        // $$ = LISTcreate();
      }
		| Parameter
		  {
        // $$ = LISTcreate();
		    // LISTadd_last($$, (Generic)$1);
      }
		| Parameters COMMA Parameter
		  {
        // $$ = $1;
		    // LISTadd_last($$, (Generic)$3);
      }
		;

Parameter	: MISSING
		  {
        // $$ = INSTANCE_NULL;
      }
		| REDEFINE
		  {
        // $$ = INSTANCE_REDEFINE;
      }
		| INTEGER
		  {
        // Instance i = INSTcreate(Type_Integer); $$ = i;
		    // i->u.integer = $1;
		    // i->symbol.line = yylineno;
      }
		| REAL
		  {
        // Instance i = INSTcreate(Type_Real); $$ = i;
		    // i->u.real = $1;
		    // i->symbol.line = yylineno;
      }
    | LOGICAL
		  {
        // Instance i = INSTcreate(Type_Logical); $$ = i;
		    // i->u.logical = $1;
		    // i->symbol.line = yylineno;
      }
		| ENUM
		  {
        // $$ = create_enum($1);
      }
		| STRING
		  {
        // Instance i = INSTcreate(Type_String); $$ = i;
		    // i->u.string = $1;
		    // i->symbol.line = yylineno;
      }
		| BINARY
		  {
        // Instance i = INSTcreate(Type_Binary); $$ = i;
		    // i->u.binary = $1;
		    // i->symbol.line = yylineno;
      }
    | Simple_Record
		  {
        // $$ = $1;
      }
		| EntityReference
		  {
        // $$ = $1.i;
      }
		| EmbeddedList
		  {
        $$ = $1;
		    /*int pos = 0;
		    $$ = INSTcreate(TYPE_AGGREGATE, &errc);
		    if (errc) ERRORreport_with_line(errc,yylineno,"INSTcreate");
		    INSTput_line_number($$, yylineno);
		    LISTdo($1, obj, Object)
			INSTaggr_at_put($$, ++pos, obj, &errc);
			if (errc != ERROR_none)
				ERRORreport_with_line(errc,
						      (obj != INSTANCE_NULL)
							? INSTget_line_number(obj)
							: yylineno,
						      pos);
		    LISTod;*/ }
		;

Entity_export   : /* NULL */
		| SLASH EntityReferences SLASH
		;

EntityReferences: EntityReference
		  {
        // DICTdefine(PREVIOUS_SCOPE,	$1.sym->name,(Generic)$1.i,
				// 		$1.sym,OBJ_INSTANCE);
      }
		| EntityReferences COMMA EntityReference
		  {
        // DICTdefine(PREVIOUS_SCOPE,	$3.sym->name,(Generic)$3.i,
				// 		$3.sym,OBJ_INSTANCE);
      }
		;

EmbeddedList	: LEFT_PAREN
		  {
        // STACKpush(listCounters, (Generic)1);
      }
		  List RIGHT_PAREN
		  {
        // (void)STACKpop(listCounters);
		    $$ = $3;
      }
		| LEFT_PAREN RIGHT_PAREN
		  {
        // $$ = INSTcreate(Type_Aggregate);
      }
		;

List            : ListEntry
		  {
        // $$ = INSTcreate(Type_Aggregate);
		    // if (errc) ERRORreport_with_line(errc,yylineno,"INSTcreate");
		    // INSTaggr_at_put($$, 1, $1);
      }
    | List COMMA ListEntry
		  {
        // int pos;
		    // pos = (int)STACKpop(listCounters);
		    // STACKpush(listCounters, (Generic)++pos);
		    // $$ = $1;
		    // INSTaggr_at_put($$, pos, $3);
		    // if (errc) ERRORreport_with_line(errc,yylineno,"INSTaggr_at_put");
      }
		;

ListEntry	: Parameter
		  { $$ = $1; }
		;
