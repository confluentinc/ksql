/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser;


import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.FailedPredicateException;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.misc.IntervalSet;

public class KSQLParserErrorStrategy extends DefaultErrorStrategy {

  public void reportError(Parser recognizer, RecognitionException e) {
    if (!this.inErrorRecoveryMode(recognizer)) {
      this.beginErrorCondition(recognizer);
      if (e instanceof NoViableAltException) {
        this.reportNoViableAlternative(recognizer, (NoViableAltException) e);
      } else if (e instanceof InputMismatchException) {
        this.reportInputMismatch(recognizer, (InputMismatchException) e);
      } else if (e instanceof FailedPredicateException) {
        this.reportFailedPredicate(recognizer, (FailedPredicateException) e);
      } else {
        System.err.println("unknown recognition error type: " + e.getClass().getName());
        recognizer.notifyErrorListeners(e.getOffendingToken(), e.getMessage(), e);
      }

    }
  }

  protected void reportNoViableAlternative(Parser recognizer, NoViableAltException e) {
    TokenStream tokens = recognizer.getInputStream();
    String input;
    if (tokens != null) {
      if (e.getStartToken().getType() == -1) {
        input = "<EOF>";
      } else {
        input = tokens.getText(e.getStartToken(), e.getOffendingToken());
      }
    } else {
      input = "<unknown input>";
    }

    String msg = "no viable alternative at input " + this.escapeWSAndQuote(input);
    recognizer.notifyErrorListeners(e.getOffendingToken(), msg, e);
  }

  protected void reportInputMismatch(Parser recognizer, InputMismatchException e) {
//        String msg = "mismatched input " + this.getTokenErrorDisplay(e.getOffendingToken()) + " expecting " + e.getExpectedTokens().toString(recognizer.getVocabulary());
    String
        msg =
        "Syntax error. There is a mismatch between the expected term and te term in the query. Please check the line and column in the query.";
    recognizer.notifyErrorListeners(e.getOffendingToken(), msg, e);
  }

  protected void reportUnwantedToken(Parser recognizer) {
    if (!this.inErrorRecoveryMode(recognizer)) {
      this.beginErrorCondition(recognizer);
      Token t = recognizer.getCurrentToken();
      String tokenName = this.getTokenErrorDisplay(t);
      IntervalSet expecting = this.getExpectedTokens(recognizer);
      String
          msg =
          "extraneous input " + tokenName + " expecting " + expecting
              .toString(recognizer.getVocabulary());
      recognizer.notifyErrorListeners(t, msg, (RecognitionException) null);
    }
  }

  protected void reportMissingToken(Parser recognizer) {
    if (!this.inErrorRecoveryMode(recognizer)) {
      this.beginErrorCondition(recognizer);
      Token t = recognizer.getCurrentToken();
      IntervalSet expecting = this.getExpectedTokens(recognizer);
      String
          msg =
          "missing " + expecting.toString(recognizer.getVocabulary()) + " at " + this
              .getTokenErrorDisplay(t);
      recognizer.notifyErrorListeners(t, msg, (RecognitionException) null);
    }
  }
}
