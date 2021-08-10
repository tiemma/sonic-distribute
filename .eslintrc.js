module.exports = {
    "env": {
        "es6": true,
        "node": true
    },
    "parser": "@typescript-eslint/parser",
    "parserOptions": {
        "project": "tsconfig.json",
        "sourceType": "module"
    },
    "plugins": [
        "eslint-plugin-import",
        '@typescript-eslint',
    ],
    "extends": ["plugin:prettier/recommended"],
    "rules": {
        "@typescript-eslint/no-inferrable-types": "error",
        "eqeqeq": [
            "error",
            "always"
        ],
        "import/order": "error",
        "no-console": "error",
        "prefer-const": "error",
        "newline-before-return": "error"
    }
};
