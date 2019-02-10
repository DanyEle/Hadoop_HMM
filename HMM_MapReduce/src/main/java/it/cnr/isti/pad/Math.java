package it.cnr.isti.pad;
/*******************************************************************************
 * Copyright (c) 2010 Haifeng Li 
 *   
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

public class Math 
{
	
	/**
     * Unitize an array so that L1 norm of x is 1.
     *
     * @param x an array of nonnegative double
     */
    public static void unitize1(double[] x) {
        double n = norm1(x);

        for (int i = 0; i < x.length; i++) {
            x[i] /= n;
        }
    }
    
    public static double norm1(double[] x) {
        double norm = 0.0;

        for (double n : x) {
            norm += Math.abs(n);
        }

        return norm;
    }
    
    public static double abs(double a) {
        return java.lang.Math.abs(a);
    }

    /**
     * Returns the absolute value of a float value.
     */
    public static float abs(float a) {
        return java.lang.Math.abs(a);
    }

    /**
     * Returns the absolute value of an int value.
     */
    public static int abs(int a) {
        return java.lang.Math.abs(a);
    }

    /**
     * Returns the absolute value of a long value.
     */
    public static long abs(long a) {
        return java.lang.Math.abs(a);
    }

    /**
     * Returns the arc cosine of an angle, in the range of 0.0 through pi.
     */
    public static double acos(double a) {
        return java.lang.Math.acos(a);
    }
    
    
    public static double max(double a, double b) {
        return java.lang.Math.max(a, b);
    }

    /**
     * Returns the greater of two float values.
     */
    public static float max(float a, float b) {
        return java.lang.Math.max(a, b);
    }

    /**
     * Returns the greater of two int values.
     */
    public static int max(int a, int b) {
        return java.lang.Math.max(a, b);
    }

    /**
     * Returns the greater of two long values.
     */
    public static long max(long a, long b) {
        return java.lang.Math.max(a, b);
    }
    
    public static double exp(double a) {
        return java.lang.Math.exp(a);
    }
    
    public static int max(int[] x) {
        int m = x[0];

        for (int n : x) {
            if (n > m) {
                m = n;
            }
        }

        return m;
    }
    
    

}
