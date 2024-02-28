/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-02-27
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#pragma once

#include <maxscale/ccdefs.hh>

#include <string>
#include <memory>
#include <vector>

#include <maxbase/jansson.hh>
#include <maxscale/pcre2.hh>
#include <maxscale/queryclassifier.hh>
#include "mysql.hh"

/**
 * @class MaskingRules
 *
 * MaskingRules abstracts the rules of a masking filter.
 */
class MaskingRules
{
    friend class MaskingRulesTester;

public:
    /**
     * @class Rule
     *
     * A Rule represents a single masking rule.
     */
    class Rule
    {
    public:
        /**
         * @class Account
         *
         * An instance of this class is capable of answering the question
         * whether the current user is subject to masking.
         */
        class Account
        {
        public:
            Account();
            virtual ~Account();

            virtual std::string user() const = 0;
            virtual std::string host() const = 0;

            /**
             * Is a user subject to masking?
             *
             * @param zUser  The name of the user.
             * @param zHost  The host of the user.
             *
             * @return True, if the data should be masked.
             */
            virtual bool matches(const char* zUser, const char* zHost) const = 0;

            /**
             * Create instance
             *
             * @param zAccount  The account name as specified in the JSON rules file.
             *
             * @return An account or null if @c zAccount is not valid.
             */
            static std::unique_ptr<MaskingRules::Rule::Account> create(const char* zAccount);
        };

        typedef std::unique_ptr<Account> SAccount;

        /**
         * Constructor of base Rule class
         *
         * @param column      The column value from the json file.
         * @param table       The table value from the json file.
         * @param database    The database value from the json file.
         * @param applies_to  Account instances corresponding to the
         *                    accounts listed in 'applies_to' in the json file.
         * @param exempted    Account instances corresponding to the
         *                    accounts listed in 'exempted' in the json file.
         */
        Rule(const std::string& column,
             const std::string& table,
             const std::string& database,
             std::vector<SAccount>&& applies_to,
             std::vector<SAccount>&& exempted);
        virtual ~Rule();

        std::string match() const;

        const std::string& column() const
        {
            return m_column;
        }
        const std::string& table() const
        {
            return m_table;
        }
        const std::string& database() const
        {
            return m_database;
        }
        const std::vector<SAccount>& applies_to() const
        {
            return m_applies_to;
        }
        const std::vector<SAccount>& exempted() const
        {
            return m_exempted;
        }

        /**
         * Establish whether a rule matches a column definition and user/host.
         *
         * @param column_def  A column definition.
         * @param zUser       The current user.
         * @param zHost       The current host.
         *
         * @return True, if the rule matches.
         */
        bool matches(const ComQueryResponse::ColumnDef& column_def,
                     const char* zUser,
                     const char* zHost) const;

        /**
         * Establish whether a rule matches a field and user/host.
         *
         * @param field  What field.
         * @param zUser  The current user.
         * @param zHost  The current host.
         *
         * @return True, if the rule matches.
         */
        bool matches(const mxs::Parser::FieldInfo& field,
                     const char* zUser,
                     const char* zHost) const;

        /**
         * Mask the column content with value or fill.
         *
         * @param s    The current value to be rewritten.
         */
        virtual void rewrite(mxq::LEncString& s) const = 0;

        /**
         * Does this rule apply to a specific account.
         *
         * @param zUser  The current user.
         * @param zHost  The current host.
         *
         * @return True, if the rule applies.
         */
        bool matches_account(const char* zUser,
                             const char* zHost) const;

    private:
        Rule(const Rule&);
        Rule& operator=(const Rule&);

    private:
        std::string           m_column;
        std::string           m_table;
        std::string           m_database;
        std::vector<SAccount> m_applies_to;
        std::vector<SAccount> m_exempted;
    };

    class ReplaceRule : public Rule
    {
    public:
        /**
         * Constructor of ReplaceRule
         *
         * @param column      The column value from the json file.
         * @param table       The table value from the json file.
         * @param database    The database value from the json file.
         * @param applies_to  Account instances corresponding to the
         *                    accounts listed in 'applies_to' in the json file.
         * @param exempted    Account instances corresponding to the
         *                    accounts listed in 'exempted' in the json file.
         * @param value       The replace value from the json file.
         * @param fill        The fill value from the json file.
         */
        ReplaceRule(const std::string& column,
                    const std::string& table,
                    const std::string& database,
                    std::vector<SAccount>&& applies_to,
                    std::vector<SAccount>&& exempted,
                    const std::string& value,
                    const std::string& fill);

        ~ReplaceRule();

        const std::string& value() const
        {
            return m_value;
        }
        const std::string& fill() const
        {
            return m_fill;
        }

        /**
         * Create a ReplaceRule instance
         *
         * @param pRule  A json object corresponding to a single
         *               rule in the rules json file.
         *
         * @return A Rule instance or NULL.
         */
        static std::unique_ptr<Rule> create_from(json_t* pRule);

        /**
         * Rewrite the column value based on rules
         *
         * @param s     The column value to rewrite.
         */
        void rewrite(mxq::LEncString& s) const override;

    private:
        std::string m_value;
        std::string m_fill;
    private:
        ReplaceRule(const ReplaceRule&);
        ReplaceRule& operator=(const ReplaceRule&);
    };

    class ObfuscateRule : public Rule
    {
    public:
        /**
         * Constructor of ObfuscateRule
         *
         * @param column      The column value from the json file.
         * @param table       The table value from the json file.
         * @param database    The database value from the json file.
         * @param applies_to  Account instances corresponding to the
         *                    accounts listed in 'applies_to' in the json file.
         * @param exempted    Account instances corresponding to the
         *                    accounts listed in 'exempted' in the json file.
         */
        ObfuscateRule(const std::string& column,
                      const std::string& table,
                      const std::string& database,
                      std::vector<SAccount>&& applies_to,
                      std::vector<SAccount>&& exempted);

        ~ObfuscateRule();

        /**
         * Create a ObfuscateRule instance
         *
         * @param pRule  A json object corresponding to a single
         *               rule in the rules json file.
         *
         * @return A Rule instance or NULL.
         */
        static std::unique_ptr<Rule> create_from(json_t* pRule);

        /**
         * Obfuscate the column value based on rules
         *
         * @param s     The column value to obfuscate.
         */
        void rewrite(mxq::LEncString& s) const override;

    private:
        ObfuscateRule(const ObfuscateRule&);
        ObfuscateRule& operator=(const ObfuscateRule&);
    };

    class MatchRule : public Rule
    {
    public:
        /**
         * Constructor of MatchRule
         *
         * @param column      The column value from the json file.
         * @param table       The table value from the json file.
         * @param database    The database value from the json file.
         * @param applies_to  Account instances corresponding to the
         *                    accounts listed in 'applies_to' in the json file.
         * @param exempted    Account instances corresponding to the
         *                    accounts listed in 'exempted' in the json file.
         * @param regexp      The compiled capture regexp from the json file.
         * @param fill        The fill value from the json file.
         */
        MatchRule(const std::string& column,
                  const std::string& table,
                  const std::string& database,
                  std::vector<SAccount>&& applies_to,
                  std::vector<SAccount>&& exempted,
                  pcre2_code* regexp,
                  const std::string& value,
                  const std::string& fill);

        ~MatchRule();

        const pcre2_code& capture() const
        {
            return *m_regexp;
        }

        const std::string& value() const
        {
            return m_value;
        }
        const std::string& fill() const
        {
            return m_fill;
        }

        /**
         * Create a MatchRule instance
         *
         * @param pRule  A json object corresponding to a single
         *               rule in the rules json file.
         *
         * @return A Rule instance or NULL.
         */
        static std::unique_ptr<Rule> create_from(json_t* pRule);

        /**
         * Rewrite the column value based on rules
         *
         * @param s     The column value to rewrite.
         */
        void rewrite(mxq::LEncString& s) const override;

    private:
        pcre2_code* m_regexp;
        std::string m_value;
        std::string m_fill;

    private:
        MatchRule(const MatchRule&);
        MatchRule& operator=(const MatchRule&);
    };

    ~MaskingRules();

    /**
     * Load rules
     *
     * @param zPath  Path to rules file.
     *
     * @return A rules object, or NULL if the rules could not be loaded.
     *         or parsed.
     */
    static std::unique_ptr<MaskingRules> load(const char* zPath);

    /**
     * Parse rules
     *
     * @param zPath  Path to rules file.
     *
     * @return A rules object, or NULL if the rules could not be parsed.
     */
    static std::unique_ptr<MaskingRules> parse(const char* zJson);

    /**
     * Create rules from JSON object.
     *
     * @param pRoot  Pointer to JSON object.
     *
     * @return A rules object, or NULL if the rules could not be created.
     */
    static std::unique_ptr<MaskingRules> create_from(json_t* pRoot);

    /**
     * Return the rule object that matches a column definition and user/host.
     *
     * @param column_def  A column definition.
     * @param zUser       The current user.
     * @param zHost       The current host.
     *
     * @return A rule object that matches the column definition and user/host
     *         or NULL if no such rule object exists.
     *
     * @attention The returned object remains value only as long as the
     *            @c MaskingRules object remains valid.
     */
    const Rule* get_rule_for(const ComQueryResponse::ColumnDef& column_def,
                             const char* zUser,
                             const char* zHost) const;

    /**
     * Return the rule object that matches a Parser::FieldInfo and user/host.
     *
     * @param field  A field.
     * @param zUser  The current user.
     * @param zHost  The current host.
     *
     * @return A rule object that matches the field indo and user/host
     *         or NULL if no such rule object exists.
     *
     * @attention The returned object remains value only as long as the
     *            @c MaskingRules object remains valid.
     */
    const Rule* get_rule_for(const mxs::Parser::FieldInfo& field_info,
                             const char* zUser,
                             const char* zHost) const;

    typedef std::unique_ptr<Rule> SRule;

    /**
     * Is there any rule for the specified user.
     *
     * @param zUser  The current user.
     * @param zHost  The current host.
     *
     * @return True, if there is a rule for that user/host combination.
     */
    bool has_rule_for(const char* zUser, const char* zHost) const;

private:
    MaskingRules(json_t* pRoot, std::vector<SRule>&& rules);

private:
    MaskingRules(const MaskingRules&);
    MaskingRules& operator=(const MaskingRules&);

private:
    json_t*            m_pRoot;
    std::vector<SRule> m_rules;
};
