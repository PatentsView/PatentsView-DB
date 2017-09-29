package lodi;

import java.io.BufferedReader;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class PatternReplacements {

    /**
     * Load a set of patterns and replacements from a {@link BufferedReader}. Each line in
     * the reader should be one of
     *     - A pattern
     *     - A blank line
     *     - A comment
     * Comments are lines beginning with the # characer. The syntax for a pattern is as
     * follows:
     *     PATTERN|REPLACEMENT|FLAGS
     *  PATTERN should be a regular expression or sequence of characters to search for.
     *  REPLACEMENT is the text that should replace PATTERN in the text. FLAGS can be used
     *  to indicate additional flags to be passed
     *  to {@link Pattern#compile}.  The following flags are currently supported:
     *      u: UNICODE_CHARACTER_CLASS
     *      L: LITERAL
     *      m: MULTILINE
     *      i: CASE_INSENSITIVE
     *
     *  If no flags are required, the line can be shortened to
     *      PATTERN|REPLACEMENT
     *  If you wish to replace the pattern with an empty string, you can write:
     *      PATTERN
     *  Note, however, that if you to set regex flags and replace the pattern with an
     *  empty string you must write
     *      PATTERN||FLAGS
     *
     * @param in A {@link BufferedReader} whose lines specify patterns and replacements
     * @return A PatternReplacements object that can applied to filter text
     */
    public static PatternReplacements loadFromFile(BufferedReader in) 
        throws java.io.IOException
    {
        PatternReplacements mr = new PatternReplacements();

        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();

            // skip comments and empty lines
            if (line.startsWith("#") || line.isEmpty())
                continue;

            String[] lineSplit = line.split("\\|");

            if (lineSplit.length == 1) {
                try {
                    mr.add(lineSplit[0]);
                }
                catch(PatternSyntaxException e) {
                    System.out.println("Pattern syntax error in line:");
                    System.out.println(line);
                }
            } 
            else {
                String pattern = lineSplit[0];
                String replacement = lineSplit[1];
                int flags = 0;

                if (lineSplit.length > 2) {
                    String f = lineSplit[2];
                    for (int i = 0; i < f.length(); i++) {
                        switch(f.charAt(i)) {
                            case 'u': flags |= Pattern.UNICODE_CHARACTER_CLASS;
                                      break;
                            case 'L': flags |= Pattern.LITERAL;
                                      break;
                            case 'm': flags |= Pattern.MULTILINE;
                                      break;
                            case 'i': flags |= Pattern.CASE_INSENSITIVE;
                                      break;
                        }
                    }
                }

                try {
                    mr.add(pattern, replacement, flags);
                }
                catch(PatternSyntaxException e) {
                    System.out.println("Pattern syntax error in line:");
                    System.out.println(line);
                }
            }
        }

        return mr;
    }

    /**
     * Create a new empty PatternReplacements object.
     */
    public PatternReplacements() {
        this.replacements = new LinkedList<>();
    }

    /**
     * Add the pattern/replacement pair with the given regex flags.
     *
     * @param pattern A String giving a regex that will be searched for in text
     * @param replacment The replacement value for when the regex is found
     * @param flags Additional flags to pass to the regex
     * @return This PatternReplacements object
     */
    public PatternReplacements add(String pattern, String replacement, int flags) {
        Pair pair = new Pair(Pattern.compile(pattern, flags), replacement);
        replacements.add(pair);
        return this;
    }

    /**
     * Add the pattern/replacement pair with no additional regex flags.
     *
     * @param pattern A String giving a regex that will be searched for in text
     * @param replacment The replacement value for when the regex is found
     * @return This PatternReplacements object
     */
    public PatternReplacements add(String pattern, String replacement) {
        return this.add(pattern, replacement, 0);
    }

    /**
     * Add the pattern with the empty string as the replacement value use the given regex
     * flags.
     *
     * @param pattern A String giving a regex that will be searched for in text
     * @param flags Additional flags to pass to the regex
     * @return This PatternReplacements object
     */
    public PatternReplacements add(String pattern, int flags) {
        return this.add(pattern, "", flags);
    }

    /**
     * Add the pattern with the empty string as the replacement value and no additional
     * regex flags.
     *
     * @param pattern A String giving a regex that will be searched for in text
     * @return This PatternReplacements object
     */
    public PatternReplacements add(String pattern) {
        return this.add(pattern, "", 0);
    }

    /**
     * Find and replace patterns in the given String. For each pattern/replacement pair in
     * this PatternReplacements object, replace all occurences in the text with the
     * replacement value.
     *
     * @param in The input string
     * @return The result of finding and replacing patterns in the input string
     */
    public String apply(String in) {
        for (Pair pair: replacements) {
            Matcher m = pair.pattern.matcher(in);
            in = m.replaceAll(pair.replacement);
        }

        return in;    
    }

    private class Pair {
        public final Pattern pattern;
        public final String replacement;

        public Pair(final Pattern pattern, final String replacement) {
            this.pattern = pattern;
            this.replacement = replacement;
        }
    }

    private final LinkedList<Pair> replacements;
}
