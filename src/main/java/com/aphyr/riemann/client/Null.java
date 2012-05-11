package com.aphyr.riemann.client;

// null has a type, but you can't use it! Since the JVM doesn't have Maybe[], we'll just make one up instead.
// The *only* acceptable argument for f(Null n) is f(null).
public class Null {
	private Null() {}
}