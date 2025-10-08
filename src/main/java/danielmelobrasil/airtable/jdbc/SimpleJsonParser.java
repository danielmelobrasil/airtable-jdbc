package danielmelobrasil.airtable.jdbc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class SimpleJsonParser {

    private final String text;
    private int pos;

    private SimpleJsonParser(String text) {
        this.text = text;
    }

    static Object parse(String json) throws IOException {
        SimpleJsonParser parser = new SimpleJsonParser(json);
        Object value = parser.parseValue();
        parser.skipWhitespace();
        if (!parser.isEnd()) {
            throw parser.error("Unexpected trailing characters");
        }
        return value;
    }

    private Object parseValue() throws IOException {
        skipWhitespace();
        if (isEnd()) {
            throw error("Unexpected end of input");
        }
        char ch = peek();
        switch (ch) {
            case '{':
                return parseObject();
            case '[':
                return parseArray();
            case '"':
                return parseString();
            case 't':
                return parseLiteral("true", Boolean.TRUE);
            case 'f':
                return parseLiteral("false", Boolean.FALSE);
            case 'n':
                return parseLiteral("null", null);
            default:
                if (ch == '-' || isDigit(ch)) {
                    return parseNumber();
                }
                throw error("Unexpected character: " + ch);
        }
    }

    private Map<String, Object> parseObject() throws IOException {
        expect('{');
        Map<String, Object> map = new LinkedHashMap<>();
        skipWhitespace();
        if (peek('}')) {
            next();
            return map;
        }
        while (true) {
            skipWhitespace();
            String key = parseString();
            skipWhitespace();
            expect(':');
            Object value = parseValue();
            map.put(key, value);
            skipWhitespace();
            if (peek(',')) {
                next();
                continue;
            }
            if (peek('}')) {
                next();
                break;
            }
            throw error("Expected ',' or '}' in object");
        }
        return map;
    }

    private List<Object> parseArray() throws IOException {
        expect('[');
        List<Object> list = new ArrayList<>();
        skipWhitespace();
        if (peek(']')) {
            next();
            return list;
        }
        while (true) {
            Object value = parseValue();
            list.add(value);
            skipWhitespace();
            if (peek(',')) {
                next();
                continue;
            }
            if (peek(']')) {
                next();
                break;
            }
            throw error("Expected ',' or ']' in array");
        }
        return list;
    }

    private String parseString() throws IOException {
        expect('"');
        StringBuilder builder = new StringBuilder();
        while (!isEnd()) {
            char ch = next();
            if (ch == '"') {
                return builder.toString();
            }
            if (ch == '\\') {
                if (isEnd()) {
                    throw error("Unexpected end in string escape");
                }
                char escaped = next();
                switch (escaped) {
                    case '"':
                    case '\\':
                    case '/':
                        builder.append(escaped);
                        break;
                    case 'b':
                        builder.append('\b');
                        break;
                    case 'f':
                        builder.append('\f');
                        break;
                    case 'n':
                        builder.append('\n');
                        break;
                    case 'r':
                        builder.append('\r');
                        break;
                    case 't':
                        builder.append('\t');
                        break;
                    case 'u':
                        builder.append(parseUnicode());
                        break;
                    default:
                        throw error("Invalid escape sequence: \\" + escaped);
                }
            } else {
                builder.append(ch);
            }
        }
        throw error("Unterminated string");
    }

    private char parseUnicode() throws IOException {
        if (pos + 4 > text.length()) {
            throw error("Invalid unicode escape");
        }
        int codePoint = 0;
        for (int i = 0; i < 4; i++) {
            char ch = text.charAt(pos++);
            int digit = Character.digit(ch, 16);
            if (digit < 0) {
                throw error("Invalid unicode escape");
            }
            codePoint = (codePoint << 4) + digit;
        }
        return (char) codePoint;
    }

    private Object parseNumber() throws IOException {
        int start = pos;
        if (peek('-')) {
            next();
        }
        if (peek('0')) {
            next();
        } else {
            if (!isDigit(peek())) {
                throw error("Invalid number");
            }
            while (!isEnd() && isDigit(peek())) {
                next();
            }
        }
        if (peek('.')) {
            next();
            if (!isDigit(peek())) {
                throw error("Invalid number");
            }
            while (!isEnd() && isDigit(peek())) {
                next();
            }
        }
        if (peek('e') || peek('E')) {
            next();
            if (peek('+') || peek('-')) {
                next();
            }
            if (!isDigit(peek())) {
                throw error("Invalid number");
            }
            while (!isEnd() && isDigit(peek())) {
                next();
            }
        }
        String number = text.substring(start, pos);
        try {
            if (number.indexOf('.') >= 0 || number.indexOf('e') >= 0 || number.indexOf('E') >= 0) {
                return Double.valueOf(number);
            }
            long longValue = Long.parseLong(number);
            if (longValue <= Integer.MAX_VALUE && longValue >= Integer.MIN_VALUE) {
                return (int) longValue;
            }
            return longValue;
        } catch (NumberFormatException ex) {
            throw error("Invalid number format: " + number);
        }
    }

    private Object parseLiteral(String literal, Object value) throws IOException {
        for (int i = 0; i < literal.length(); i++) {
            if (isEnd() || text.charAt(pos++) != literal.charAt(i)) {
                throw error("Expected literal: " + literal);
            }
        }
        return value;
    }

    private void skipWhitespace() {
        while (!isEnd()) {
            char ch = peek();
            if (ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t') {
                pos++;
            } else {
                break;
            }
        }
    }

    private boolean peek(char expected) {
        return !isEnd() && text.charAt(pos) == expected;
    }

    private char peek() {
        return text.charAt(pos);
    }

    private char next() {
        return text.charAt(pos++);
    }

    private void expect(char expected) throws IOException {
        if (isEnd() || text.charAt(pos++) != expected) {
            throw error("Expected '" + expected + "'");
        }
    }

    private boolean isEnd() {
        return pos >= text.length();
    }

    private static boolean isDigit(char ch) {
        return ch >= '0' && ch <= '9';
    }

    private IOException error(String message) {
        return new IOException(message + " at position " + pos);
    }
}
