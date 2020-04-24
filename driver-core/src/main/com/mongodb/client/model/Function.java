/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model;

import com.mongodb.lang.Nullable;
import java.util.List;

/**
 * Defines a Function for use in $function pipeline stages.
 *
 * @mongodb.server.release 4.4
 * @since 4.1
 */
public class Function {
    private final String functionBody;
    private final List<String> functionArgs;
    private final String lang;

    /**
     * Create a Function that takes no arguments and uses the default language.
     *
     * @param functionBody   the body of the function
     */
    public Function(final String functionBody) {
        this(functionBody, null);
    }

    /**
     * Create a Function that takes arguments and uses the default language.
     *
     * @param functionBody   the body of the function
     * @param functionArgs   the arguments to the function
     */
    public Function(final String functionBody, @Nullable final List<String> functionArgs) {
        this(functionBody, functionArgs, "js");
    }

    /**
     * @param functionBody   the body of the function
     * @param functionArgs   the arguments to the function
     * @param lang           a language specifier; the only valid value currently is "js"
     */
    public Function(final String functionBody, @Nullable final List<String> functionArgs, final String lang) {
        this.functionBody = functionBody;
        this.functionArgs = functionArgs;
        this.lang = lang;
    }

    /**
     * Get the function body.
     *
     * @return the function body
     */
    public String getFunctionBody() {
        return functionBody;
    }

    /**
     * Get the list of function arguments.
     *
     * @return the list of function arguments. Returns null if there are no function arguments.
     */
    public List<String> getFunctionArgs() {
        return functionArgs;
    }

    /**
     * Get the language specifier for the function.
     *
     * @return the language specifier
     */
    public String getLang() {
        return lang;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Function function = (Function) o;

        if (!functionBody.equals(function.functionBody)) {
            return false;
        }
        if (!lang.equals(function.lang)) {
            return false;
        }
        return functionArgs != null ? functionArgs.equals(function.functionArgs) : function.functionArgs == null;
    }

    @Override
    public int hashCode() {
        int result = functionBody.hashCode();
        result = 31 * result + (functionArgs != null ? functionArgs.hashCode() : 0);
        result = 31 * result + lang.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Function{"
                + "body='" + functionBody + '\''
                + ", functionArgs=" + functionArgs
                + ", lang=" + lang
                + '}';
    }
}
