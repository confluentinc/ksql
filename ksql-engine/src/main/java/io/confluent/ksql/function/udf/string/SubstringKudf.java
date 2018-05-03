/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.function.udf.string;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;

public class SubstringKudf implements Kudf {

  @Override
  public Object evaluate(Object... args) {
    if ((args.length < 2) || (args.length > 3)) {
      throw new KsqlFunctionException("Substring udf should have two or three input argument.");
    }
    
    if(args[0]!=null && args[1]!=null) {
    	
    	String string = args[0].toString();
	    int start = Integer.parseInt(args[1].toString());
	    if (args.length == 2) {
	    	if(start < 0 || start > string.length()) {
	    		throw new KsqlFunctionException("Substring udf start position smaller zero or greater length of string.");
	    	}
	    	return string.substring(start);
	    } else {
	      if(args[2]!=null) {
	      	int end = Integer.parseInt(args[1].toString());
	      	if(end < 0 || end > string.length()) {
	    		throw new KsqlFunctionException("Substring udf end position smaller zero or greater length of string.");
	      	}
	      	return string.substring(start, end);
	      } else {
	      	return null;
	      }
	    }
    } else {
    	return null;
    }
  }
}
