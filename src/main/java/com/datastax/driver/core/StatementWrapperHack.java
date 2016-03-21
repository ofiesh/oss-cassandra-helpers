package com.datastax.driver.core;

/**
 * <p>
 * This is not ideal; {@link StatementWrapper} ought to have getWrappedStatement as a public getter.
 * </p>
 * 
 * <p>
 * Okay, let's be honest: this is an ugly hack.
 * </p>
 * 
 * <li>TODO: submit a driver patch which makes {@link StatementWrapper#getWrappedStatement()} public
 * <li>TODO: get rid of this ugly hack.
 * 
 * @author eehlinger
 *
 */
public class StatementWrapperHack {

    public static Statement getWrappedStatement(StatementWrapper source) {
        return source.getWrappedStatement();
    }

}
