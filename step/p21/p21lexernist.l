%{

package p21

import "fmt"

var yylineno int

var open_comment int /* positive number is the line where the comment began */

// int yywrap()
// {
// 	if (open_comment) {
// 		ERRORreport_with_line(ERROR_unmatched_open_comment,
// 			open_comment);
// 	}
// 	return 1;
// }

type (
	term     int
	keywords map[text]term
)

var keyWordTypes = keywords{
	"DATA":     DATA,
	"ENDSCOPE": ENDSCOPE,
	"ENDSEC":   ENDSEC,
	/*	"END_ISO_10303_21":	END_ISO_10303_21,*/
	"HEADER": HEADER,
	/*	"ISO_10303_21":		ISO_10303_21,*/
}

func findIdent(table keywords, s text) term {
	if k, ok := table[s]; ok {
		return k
	}
	return KEYWORD
}

type startConditionT int

const (
	code startConditionT = iota
	comment
)

func lex() term {
	var startCondition startConditionT
	BEGIN := func(sc startConditionT) { startCondition = sc }
	var c int
	getc := func() int { return 0 }

	BEGIN(code)
%}

%yyc c
%yyn c = getc()
%yyt startCondition

%s code comment

digit		[0-9]
integer		{digit}+
entityName	{digit}{1,9}
ident		[A-Z][A-Z0-9_]*
quote		\"

%%

[\t ]*		;
\n		{ yylineno++ }

<code>"/*"		{	// new-style comments "!*""!"*([^*!]|[^*]"!"|"*"[^!])*"*"+"!"
		open_comment = yylineno
		BEGIN(comment)
	}

<code>"!*"		{	/* old-style comments */
		// fprintf(stderr, "WARNING <line %4d>: Old-style comment\n", yylineno)
		open_comment = yylineno
		BEGIN(comment);
	}

<code>".F."		{
		// yylval.lVal = Lfalse;
		return LOGICAL;
	}

<code>".U."		{
		// yylval.lVal = Lunknown;
		return LOGICAL;
	}

<code>".T."		{
		// yylval.lVal = Ltrue;
		return LOGICAL;
	}

<code>"."{ident}"."	{
		// yylval.string = SCANstrdup(yytext+1);	/* remove 1st dot */
		// *strrchr(yylval.string,'.') = 0;	/* remove 2nd */
		// SCANlowerize(yylval.string);
		return ENUM;
	}

<code>"!"{ident}	{	/* user-defined keyword */
		// yylval.string = SCANstrdup(yytext + 1);
		// SCANlowerize(yylval.string);
		return USER_DEFINED_KEYWORD;
	}

<code>"#"{entityName}	{
                // yylval.string = SCANstrdup(yytext + 1);
                return ENTITY_NAME;
	}

<code>[+-]?{integer}"."{integer}?("E"[+-]?{integer})?	{
		// sscanf(yytext, "%lf", &(yylval.rVal));
                return REAL;
	}

<code>[-+]?{integer}	{
		// sscanf(yytext, "%d", &(yylval.iVal));
		return INTEGER;
	}

<code>'([^'\n]|'')*'	{
		// char *s, *d;	/* source, destination */

		// yylval.string = SCANstrdup(yytext+1);	/* remove 1st quote */
		// *strrchr(yylval.string,'\'') = 0;	/* remove 2nd quote which is */
		// 			/* is guaranteed to exist by pattern above */

		// /* do it by hand rather than say strstr because we don't want to */
		// /* rescan and pick up new \X\s than were created when olds ones */
		// /* were removed - i.e., \\X\Y\ - DEL */
		// for (s = d = yylval.string;*s;) {
		// 	if (*s != '\'' && *s != '\\') {
		// 		*d++ = *s++;
		// 	} else if (0 == strncmp(s,"\\\\N\\\\",2) ||
		// 		   0 == strncmp(s,"\\\\F\\\\",2)) {
		// 		s += 5;
		// 	} else if (0 == strncmp(s,"''",2) ||
		// 		   0 == strncmp(s,"\\\\",2)) {
		// 		*d++ = *s;
		// 		s += 2;
		// 	}
		// }
		// *d = '\0';

		return STRING;
	}

<code>'([^'\n]|'')*\n	{
		// ERRORreport_with_line(ERROR_unterminated_string, yylineno);
		yylineno++
		// yylval.string = SCANstrdup(yytext+1);	/* remove 1st quote */

		return STRING;
	}

<code>{quote}[0-3][0-9A-F]*{quote}	{
		// yylval.string = SCANstrdup(yytext+1);	/* remove 1st quote */
		// *strrchr(yylval.string,'"') = 0;	/* remove 2nd quote which is */
		// 			/* is guaranteed to exist by pattern above */
		return BINARY;
	}

<code>"&SCOPE"			{return SCOPE;}
<code>"ISO-10303-21"		{return ISO_10303_21;}
<code>"END-ISO-10303-21"	{return END_ISO_10303_21;}

<code>{ident}		{ 			/* keyword */
                // yylval.string = SCANstrdup(yytext);
		return findIdent(keyWordTypes, ""/*yytext*/);
	}

<code>"="		return EQUALS;

<code>";"		return SEMICOLON;

<code>","		return COMMA;

<code>"("		return LEFT_PAREN;

<code>")"		return RIGHT_PAREN;

<code>"/"		return SLASH;

<code>"$"		return MISSING;

<code>"*"		return REDEFINE;

<code>\\N\\		/* print control directives - DEL */ ;
<code>\\F\\		;

<comment>"*/"		{
		open_comment = 0
		BEGIN(code)
	}

<code>"*/"		{
		// ERRORreport_with_line(ERROR_unmatched_close_comment,yylineno);
	}


<comment>[^*\n]*	;
<comment>[*]		;
[a-z]			{/*ERRORreport_with_line(ERROR_illegal_lowercase,
						yylineno,yytext[0]);*/}
.			{/*ERRORreport_with_line(ERROR_illegal_character,
						yylineno,(int)yytext[0]);*/}
%%
	panic("should not reach this point")
}
