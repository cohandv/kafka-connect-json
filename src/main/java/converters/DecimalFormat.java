package converters;

public enum DecimalFormat {

    /**
     * Serializes the JSON Decimal as a base-64 string. For example, serializing the value
     * `10.2345` with the BASE64 setting will result in `"D3J5"`.
     */
    BASE64,

    /**
     * Serializes the JSON Decimal as a JSON number. For example, serializing the value
     * `10.2345` with the NUMERIC setting will result in `10.2345`.
     */
    NUMERIC
}
