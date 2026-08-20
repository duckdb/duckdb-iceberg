#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

//! Whether an identifier container was constructed for an explicit case_sensitive ATTACH
//! option, or is falling back to today's (per-field) default behavior because the option
//! was left unset. UNSET and INSENSITIVE both use the case-insensitive backend container,
//! but only INSENSITIVE raises an ambiguity error on a same-fold insert collision - UNSET
//! must silently collide exactly as it does today, since "unset changes nothing".
enum class CaseSensitivityMode : uint8_t { UNSET, SENSITIVE, INSENSITIVE };

//! A map keyed by identifier name, whose case-folding behavior is fixed at construction:
//! - SENSITIVE: backed by a plain unordered_map<string, T> (case-distinct keys coexist).
//! - UNSET or INSENSITIVE: backed by DuckDB's own case_insensitive_map_t<T>.
template <class T>
class CaseAwareIdentifierMap {
public:
	using SensitiveMap = unordered_map<string, T>;
	using InsensitiveMap = case_insensitive_map_t<T>;
	using value_type = std::pair<const string, T>;

	//! Note: on some standard library implementations, SensitiveMap::iterator and
	//! InsensitiveMap::iterator are the *same* underlying type (the node/iterator type for
	//! unordered_map does not always depend on the Hash/KeyEqual template parameters). This
	//! means two constructors overloaded only on these two parameter types can collide as a
	//! redeclaration. To stay portable regardless of whether a given std lib unifies them or
	//! not, construction is done through two distinctly-*named* static factories instead of
	//! overloading on parameter type.
	class iterator {
	public:
		iterator() : is_sensitive(true) {
		}

		static iterator MakeSensitive(typename SensitiveMap::iterator it) {
			iterator result;
			result.is_sensitive = true;
			result.sensitive_it = it;
			return result;
		}
		static iterator MakeInsensitive(typename InsensitiveMap::iterator it) {
			iterator result;
			result.is_sensitive = false;
			result.insensitive_it = it;
			return result;
		}

		value_type &operator*() const {
			return is_sensitive ? *sensitive_it : *insensitive_it;
		}
		value_type *operator->() const {
			return is_sensitive ? &(*sensitive_it) : &(*insensitive_it);
		}
		iterator &operator++() {
			if (is_sensitive) {
				++sensitive_it;
			} else {
				++insensitive_it;
			}
			return *this;
		}
		bool operator==(const iterator &other) const {
			if (is_sensitive != other.is_sensitive) {
				return false;
			}
			return is_sensitive ? sensitive_it == other.sensitive_it : insensitive_it == other.insensitive_it;
		}
		bool operator!=(const iterator &other) const {
			return !(*this == other);
		}

	private:
		bool is_sensitive;
		typename SensitiveMap::iterator sensitive_it;
		typename InsensitiveMap::iterator insensitive_it;
	};

	//! Const-iteration counterpart of 'iterator', for use from const methods/contexts.
	class const_iterator {
	public:
		const_iterator() : is_sensitive(true) {
		}

		static const_iterator MakeSensitive(typename SensitiveMap::const_iterator it) {
			const_iterator result;
			result.is_sensitive = true;
			result.sensitive_it = it;
			return result;
		}
		static const_iterator MakeInsensitive(typename InsensitiveMap::const_iterator it) {
			const_iterator result;
			result.is_sensitive = false;
			result.insensitive_it = it;
			return result;
		}

		const value_type &operator*() const {
			return is_sensitive ? *sensitive_it : *insensitive_it;
		}
		const value_type *operator->() const {
			return is_sensitive ? &(*sensitive_it) : &(*insensitive_it);
		}
		const_iterator &operator++() {
			if (is_sensitive) {
				++sensitive_it;
			} else {
				++insensitive_it;
			}
			return *this;
		}
		bool operator==(const const_iterator &other) const {
			if (is_sensitive != other.is_sensitive) {
				return false;
			}
			return is_sensitive ? sensitive_it == other.sensitive_it : insensitive_it == other.insensitive_it;
		}
		bool operator!=(const const_iterator &other) const {
			return !(*this == other);
		}

	private:
		bool is_sensitive;
		typename SensitiveMap::const_iterator sensitive_it;
		typename InsensitiveMap::const_iterator insensitive_it;
	};

	explicit CaseAwareIdentifierMap(CaseSensitivityMode mode_p = CaseSensitivityMode::UNSET) : mode(mode_p) {
	}

public:
	iterator begin() {
		return mode == CaseSensitivityMode::SENSITIVE ? iterator::MakeSensitive(sensitive_map.begin())
		                                              : iterator::MakeInsensitive(insensitive_map.begin());
	}
	iterator end() {
		return mode == CaseSensitivityMode::SENSITIVE ? iterator::MakeSensitive(sensitive_map.end())
		                                              : iterator::MakeInsensitive(insensitive_map.end());
	}
	const_iterator begin() const {
		return mode == CaseSensitivityMode::SENSITIVE ? const_iterator::MakeSensitive(sensitive_map.begin())
		                                              : const_iterator::MakeInsensitive(insensitive_map.begin());
	}
	const_iterator end() const {
		return mode == CaseSensitivityMode::SENSITIVE ? const_iterator::MakeSensitive(sensitive_map.end())
		                                              : const_iterator::MakeInsensitive(insensitive_map.end());
	}

	iterator find(const string &key) {
		return mode == CaseSensitivityMode::SENSITIVE ? iterator::MakeSensitive(sensitive_map.find(key))
		                                              : iterator::MakeInsensitive(insensitive_map.find(key));
	}
	const_iterator find(const string &key) const {
		return mode == CaseSensitivityMode::SENSITIVE ? const_iterator::MakeSensitive(sensitive_map.find(key))
		                                              : const_iterator::MakeInsensitive(insensitive_map.find(key));
	}

	bool contains(const string &key) const { // NOLINT: match STL container naming
		return find(key) != end();
	}

	idx_t count(const string &key) const { // NOLINT: match STL container naming
		return contains(key) ? 1 : 0;
	}

	bool empty() const { // NOLINT: match STL container naming
		return mode == CaseSensitivityMode::SENSITIVE ? sensitive_map.empty() : insensitive_map.empty();
	}

	idx_t size() const { // NOLINT: match STL container naming
		return mode == CaseSensitivityMode::SENSITIVE ? sensitive_map.size() : insensitive_map.size();
	}

	T &at(const string &key) { // NOLINT: match STL container naming
		return mode == CaseSensitivityMode::SENSITIVE ? sensitive_map.at(key) : insensitive_map.at(key);
	}

	//! Mirrors unordered_map::operator[] - intended for overwriting/rolling back an already
	//! resolved key (a slot that's known to exist, or that the caller wants default-created).
	//! Deliberately bypasses the ambiguity check in EmplaceInternal, matching operator[]'s own
	//! "just give me a slot for this exact key" semantics rather than "insert a new identifier".
	T &operator[](const string &key) {
		return mode == CaseSensitivityMode::SENSITIVE ? sensitive_map[key] : insensitive_map[key];
	}

	std::pair<iterator, bool> insert(const string &key, T value) { // NOLINT: match STL container naming
		return EmplaceInternal(key, std::move(value));
	}

	std::pair<iterator, bool> emplace(const string &key, T value) { // NOLINT: match STL container naming
		return EmplaceInternal(key, std::move(value));
	}

	void erase(const string &key) { // NOLINT: match STL container naming
		if (mode == CaseSensitivityMode::SENSITIVE) {
			sensitive_map.erase(key);
		} else {
			insensitive_map.erase(key);
		}
	}
	//! Returns the iterator following the erased element, matching STL's erase(iterator) convention.
	//! Erasing by key first (rather than reaching into 'it's private members) is safe here: erasing
	//! one key from an unordered_map never invalidates iterators to any other element.
	iterator erase(iterator it) { // NOLINT: match STL container naming
		auto next = it;
		++next;
		erase(it->first);
		return next;
	}

	void clear() { // NOLINT: match STL container naming
		if (mode == CaseSensitivityMode::SENSITIVE) {
			sensitive_map.clear();
		} else {
			insensitive_map.clear();
		}
	}

private:
	std::pair<iterator, bool> EmplaceInternal(const string &key, T value) {
		if (mode == CaseSensitivityMode::SENSITIVE) {
			auto res = sensitive_map.emplace(key, std::move(value));
			return {iterator::MakeSensitive(res.first), res.second};
		}
		//! UNSET and INSENSITIVE share the case-insensitive backend; only INSENSITIVE
		//! (explicitly requested) treats a same-fold/different-exact-string collision as
		//! an error rather than a silent first-wins insert.
		auto existing = insensitive_map.find(key);
		if (mode == CaseSensitivityMode::INSENSITIVE && existing != insensitive_map.end() && existing->first != key) {
			throw CatalogException("Ambiguous case-insensitive identifier '%s': matches existing identifier '%s'. Use "
			                       "case_sensitive=true or an exact-case reference to disambiguate.",
			                       key, existing->first);
		}
		auto res = insensitive_map.emplace(key, std::move(value));
		return {iterator::MakeInsensitive(res.first), res.second};
	}

private:
	CaseSensitivityMode mode;
	SensitiveMap sensitive_map;
	InsensitiveMap insensitive_map;
};

//! A set of identifier names, whose case-folding behavior is fixed at construction, mirroring
//! CaseAwareIdentifierMap's semantics.
class CaseAwareIdentifierSet {
public:
	using SensitiveSet = unordered_set<string>;
	using InsensitiveSet = case_insensitive_set_t;

	//! See the comment on CaseAwareIdentifierMap::iterator - named factories instead of
	//! constructors overloaded on parameter type, since SensitiveSet::iterator and
	//! InsensitiveSet::iterator may be the same underlying type on some std libs.
	class iterator {
	public:
		iterator() : is_sensitive(true) {
		}

		static iterator MakeSensitive(SensitiveSet::iterator it) {
			iterator result;
			result.is_sensitive = true;
			result.sensitive_it = it;
			return result;
		}
		static iterator MakeInsensitive(InsensitiveSet::iterator it) {
			iterator result;
			result.is_sensitive = false;
			result.insensitive_it = it;
			return result;
		}

		const string &operator*() const {
			return is_sensitive ? *sensitive_it : *insensitive_it;
		}
		iterator &operator++() {
			if (is_sensitive) {
				++sensitive_it;
			} else {
				++insensitive_it;
			}
			return *this;
		}
		bool operator==(const iterator &other) const {
			if (is_sensitive != other.is_sensitive) {
				return false;
			}
			return is_sensitive ? sensitive_it == other.sensitive_it : insensitive_it == other.insensitive_it;
		}
		bool operator!=(const iterator &other) const {
			return !(*this == other);
		}

	private:
		bool is_sensitive;
		SensitiveSet::iterator sensitive_it;
		InsensitiveSet::iterator insensitive_it;
	};

	explicit CaseAwareIdentifierSet(CaseSensitivityMode mode_p = CaseSensitivityMode::UNSET) : mode(mode_p) {
	}

public:
	iterator begin() {
		return mode == CaseSensitivityMode::SENSITIVE ? iterator::MakeSensitive(sensitive_set.begin())
		                                              : iterator::MakeInsensitive(insensitive_set.begin());
	}
	iterator end() {
		return mode == CaseSensitivityMode::SENSITIVE ? iterator::MakeSensitive(sensitive_set.end())
		                                              : iterator::MakeInsensitive(insensitive_set.end());
	}

	bool contains(const string &key) const { // NOLINT: match STL container naming
		return mode == CaseSensitivityMode::SENSITIVE ? sensitive_set.count(key) > 0 : insensitive_set.count(key) > 0;
	}

	idx_t count(const string &key) const { // NOLINT: match STL container naming
		return contains(key) ? 1 : 0;
	}

	bool empty() const { // NOLINT: match STL container naming
		return mode == CaseSensitivityMode::SENSITIVE ? sensitive_set.empty() : insensitive_set.empty();
	}

	//! Returns true if the identifier was newly inserted (mirrors std::set::insert's .second).
	//! Raises the same ambiguity error as CaseAwareIdentifierMap::insert (only in explicit
	//! INSENSITIVE mode) - use this for sets that track real, distinct identifiers (e.g.
	//! created/deleted schema names), where a same-fold collision represents a genuine conflict.
	bool insert(const string &key) { // NOLINT: match STL container naming
		if (mode == CaseSensitivityMode::SENSITIVE) {
			return sensitive_set.insert(key).second;
		}
		auto existing = insensitive_set.find(key);
		if (mode == CaseSensitivityMode::INSENSITIVE && existing != insensitive_set.end() && *existing != key) {
			throw CatalogException("Ambiguous case-insensitive identifier '%s': matches existing identifier '%s'. Use "
			                       "case_sensitive=true or an exact-case reference to disambiguate.",
			                       key, *existing);
		}
		return insensitive_set.insert(key).second;
	}

	//! Like insert, but never raises the ambiguity error - for memoization-style sets (e.g.
	//! "have we already attempted to resolve this name") where a same-fold collision isn't a
	//! real identity conflict, just two spellings recording the same already-attempted lookup.
	bool insert_permissive(const string &key) { // NOLINT: match STL container naming
		if (mode == CaseSensitivityMode::SENSITIVE) {
			return sensitive_set.insert(key).second;
		}
		return insensitive_set.insert(key).second;
	}

	void erase(const string &key) { // NOLINT: match STL container naming
		if (mode == CaseSensitivityMode::SENSITIVE) {
			sensitive_set.erase(key);
		} else {
			insensitive_set.erase(key);
		}
	}

	void clear() { // NOLINT: match STL container naming
		if (mode == CaseSensitivityMode::SENSITIVE) {
			sensitive_set.clear();
		} else {
			insensitive_set.clear();
		}
	}

private:
	CaseSensitivityMode mode;
	SensitiveSet sensitive_set;
	InsensitiveSet insensitive_set;
};

} // namespace duckdb
