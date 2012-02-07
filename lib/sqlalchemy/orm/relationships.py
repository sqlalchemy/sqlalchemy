# orm/relationships.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Heuristics related to join conditions as used in 
:func:`.relationship`.

Provides the :class:`.JoinCondition` object, which encapsulates
SQL annotation and aliasing behavior focused on the `primaryjoin`
and `secondaryjoin` aspects of :func:`.relationship`.

"""

from sqlalchemy import sql, util, log, exc as sa_exc, schema
from sqlalchemy.sql.util import ClauseAdapter, criterion_as_pairs, \
    join_condition, _shallow_annotate
from sqlalchemy.sql import operators, expression, visitors
from sqlalchemy.orm.interfaces import MANYTOMANY, MANYTOONE, ONETOMANY

class JoinCondition(object):
    def __init__(self, 
                    parent_selectable, 
                    child_selectable,
                    parent_local_selectable,
                    child_local_selectable,
                    primaryjoin=None,
                    secondary=None,
                    secondaryjoin=None,
                    parent_equivalents=None,
                    child_equivalents=None,
                    consider_as_foreign_keys=None,
                    local_remote_pairs=None,
                    remote_side=None,
                    self_referential=False,
                    prop=None,
                    support_sync=True,
                    can_be_synced_fn=lambda c: True
                    ):
        self.parent_selectable = parent_selectable
        self.parent_local_selectable = parent_local_selectable
        self.child_selectable = child_selectable
        self.child_local_selectable = child_local_selectable
        self.parent_equivalents = parent_equivalents
        self.child_equivalents = child_equivalents
        self.primaryjoin = primaryjoin
        self.secondaryjoin = secondaryjoin
        self.secondary = secondary
        self.consider_as_foreign_keys = consider_as_foreign_keys
        self._local_remote_pairs = local_remote_pairs
        self._remote_side = remote_side
        self.prop = prop
        self.self_referential = self_referential
        self.support_sync = support_sync
        self.can_be_synced_fn = can_be_synced_fn
        self._determine_joins()
        self._parse_joins()
        self._determine_direction()

    def _determine_joins(self):
        """Determine the 'primaryjoin' and 'secondaryjoin' attributes,
        if not passed to the constructor already.

        This is based on analysis of the foreign key relationships
        between the parent and target mapped selectables.

        """
        if self.secondaryjoin is not None and self.secondary is None:
            raise sa_exc.ArgumentError(
                    "Property %s specified with secondary "
                    "join condition but "
                    "no secondary argument" % self.prop)

        # find a join between the given mapper's mapped table and
        # the given table. will try the mapper's local table first
        # for more specificity, then if not found will try the more
        # general mapped table, which in the case of inheritance is
        # a join.
        try:
            if self.secondary is not None:
                if self.secondaryjoin is None:
                    self.secondaryjoin = \
                        join_condition(
                                self.child_selectable, 
                                self.secondary,
                                a_subset=self.child_local_selectable)
                if self.primaryjoin is None:
                    self.primaryjoin = \
                        join_condition(
                                self.parent_selectable, 
                                self.secondary, 
                                a_subset=self.parent_local_selectable)
            else:
                if self.primaryjoin is None:
                    self.primaryjoin = \
                        join_condition(
                                self.parent_selectable, 
                                self.child_selectable, 
                                a_subset=self.parent_local_selectable)
        except sa_exc.ArgumentError, e:
            raise sa_exc.ArgumentError("Could not determine join "
                    "condition between parent/child tables on "
                    "relationship %s.  Specify a 'primaryjoin' "
                    "expression.  If 'secondary' is present, "
                    "'secondaryjoin' is needed as well."
                    % self.prop)

    def _parse_joins(self):
        """Apply 'remote', 'local' and 'foreign' annotations
        to the primary and secondary join conditions.

        """
        parentcols = util.column_set(self.parent_selectable.c)
        targetcols = util.column_set(self.child_selectable.c)
        if self.secondary is not None:
            secondarycols = util.column_set(self.secondary.c)
        else:
            secondarycols = set()

        def col_is(a, b):
            return a.compare(b)

        def refers_to_parent_table(binary):
            pt = self.parent_selectable
            mt = self.child_selectable
            c, f = binary.left, binary.right
            if (
                pt.is_derived_from(c.table) and \
                pt.is_derived_from(f.table) and \
                mt.is_derived_from(c.table) and \
                mt.is_derived_from(f.table)
            ):
                return True
            else:
                return False

        def is_foreign(a, b):
            if self.consider_as_foreign_keys:
                if a in self.consider_as_foreign_keys and (
                        col_is(a, b) or 
                        b not in self.consider_as_foreign_keys
                    ):
                    return a
                elif b in self.consider_as_foreign_keys and (
                        col_is(a, b) or 
                        a not in self.consider_as_foreign_keys
                    ):
                    return b
            elif isinstance(a, schema.Column) and \
                        isinstance(b, schema.Column):
                if a.references(b):
                    return a
                elif b.references(a):
                    return b
            elif secondarycols:
                if a in secondarycols and b not in secondarycols:
                    return a
                elif b in secondarycols and a not in secondarycols:
                    return b

        def _annotate_fk(binary, switch):
            if switch:
                right, left = binary.left, binary.right
            else:
                left, right = binary.left, binary.right
            can_be_synced = self.can_be_synced_fn(left)
            left = left._annotate({
                "equated":binary.operator is operators.eq,
                "can_be_synced":can_be_synced and \
                    binary.operator is operators.eq
            })
            right = right._annotate({
                "equated":binary.operator is operators.eq,
                "referent":True
            })
            if switch:
                binary.right, binary.left = left, right
            else:
                binary.left, binary.right = left, right

        def _annotate_remote(binary, switch):
            if switch:
                right, left = binary.left, binary.right
            else:
                left, right = binary.left, binary.right
            left = left._annotate(
                                {"remote":True})
            if right in parentcols or \
                secondarycols and right in targetcols:
                right = right._annotate(
                                {"local":True})
            if switch:
                binary.right, binary.left = left, right
            else:
                binary.left, binary.right = left, right

        def visit_binary(binary):
            if not isinstance(binary.left, sql.ColumnElement) or \
                        not isinstance(binary.right, sql.ColumnElement):
                return

            if "foreign" not in binary.left._annotations and \
                "foreign" not in binary.right._annotations:
                col = is_foreign(binary.left, binary.right)
                if col is not None:
                    if col is binary.left:
                        binary.left = binary.left._annotate(
                                            {"foreign":True})
                    elif col is binary.right:
                        binary.right = binary.right._annotate(
                                            {"foreign":True})
                    # TODO: when the two cols are the same.

            has_foreign = False
            if "foreign" in binary.left._annotations:
                _annotate_fk(binary, False)
                has_foreign = True
            if "foreign" in binary.right._annotations:
                _annotate_fk(binary, True)
                has_foreign = True

            if "remote" not in binary.left._annotations and \
                "remote" not in binary.right._annotations:
                if self._local_remote_pairs:
                    raise NotImplementedError()
                elif self._remote_side:
                    if binary.left in self._remote_side:
                        _annotate_remote(binary, False)
                    elif binary.right in self._remote_side:
                        _annotate_remote(binary, True)
                elif refers_to_parent_table(binary):
                    # assume one to many - FKs are "remote"
                    if "foreign" in binary.left._annotations:
                        _annotate_remote(binary, False)
                    elif "foreign" in binary.right._annotations:
                        _annotate_remote(binary, True)
                elif secondarycols:
                    if binary.left in secondarycols:
                        _annotate_remote(binary, False)
                    elif binary.right in secondarycols:
                        _annotate_remote(binary, True)
                else:
                    if binary.left in targetcols and has_foreign:
                        _annotate_remote(binary, False)
                    elif binary.right in targetcols and has_foreign:
                        _annotate_remote(binary, True)

        self.primaryjoin = visitors.cloned_traverse(
            self.primaryjoin,
            {},
            {"binary":visit_binary}
        )
        if self.secondaryjoin is not None:
            self.secondaryjoin = visitors.cloned_traverse(
                self.secondaryjoin,
                {},
                {"binary":visit_binary}
            )
        self._check_foreign_cols(
                        self.primaryjoin, True)
        if self.secondaryjoin is not None:
            self._check_foreign_cols(
                        self.secondaryjoin, False)


    def _check_foreign_cols(self, join_condition, primary):
        """Check the foreign key columns collected and emit error messages."""
        # TODO: don't worry, we can simplify this once we
        # encourage configuration via direct annotation

        can_sync = False

        foreign_cols = self._gather_columns_with_annotation(
                                join_condition, "foreign")

        has_foreign = bool(foreign_cols)

        if self.support_sync:
            for col in foreign_cols:
                if col._annotations.get("can_be_synced"):
                    can_sync = True
                    break

        if self.support_sync and can_sync or \
            (not self.support_sync and has_foreign):
            return

        # from here below is just determining the best error message
        # to report.  Check for a join condition using any operator 
        # (not just ==), perhaps they need to turn on "viewonly=True".
        if self.support_sync and has_foreign and not can_sync:

            err = "Could not locate any "\
                    "foreign-key-equated, locally mapped column "\
                    "pairs for %s "\
                    "condition '%s' on relationship %s." % (
                        primary and 'primaryjoin' or 'secondaryjoin', 
                        join_condition, 
                        self.prop
                    )

            # TODO: this needs to be changed to detect that
            # annotations were present and whatnot.   the future
            # foreignkey(col) annotation will cover establishing
            # the col as foreign to it's mate
            if not self.consider_as_foreign_keys:
                err += "  Ensure that the "\
                        "referencing Column objects have a "\
                        "ForeignKey present, or are otherwise part "\
                        "of a ForeignKeyConstraint on their parent "\
                        "Table, or specify the foreign_keys parameter "\
                        "to this relationship."

            err += "  For more "\
                    "relaxed rules on join conditions, the "\
                    "relationship may be marked as viewonly=True."

            raise sa_exc.ArgumentError(err)
        else:
            if self.consider_as_foreign_keys:
                raise sa_exc.ArgumentError("Could not determine "
                        "relationship direction for %s condition "
                        "'%s', on relationship %s, using manual "
                        "'foreign_keys' setting.  Do the columns "
                        "in 'foreign_keys' represent all, and "
                        "only, the 'foreign' columns in this join "
                        "condition?  Does the %s Table already "
                        "have adequate ForeignKey and/or "
                        "ForeignKeyConstraint objects established "
                        "(in which case 'foreign_keys' is usually "
                        "unnecessary)?" 
                        % (
                            primary and 'primaryjoin' or 'secondaryjoin',
                            join_condition, 
                            self.prop,
                            primary and 'mapped' or 'secondary'
                        ))
            else:
                raise sa_exc.ArgumentError("Could not determine "
                        "relationship direction for %s condition "
                        "'%s', on relationship %s. Ensure that the "
                        "referencing Column objects have a "
                        "ForeignKey present, or are otherwise part "
                        "of a ForeignKeyConstraint on their parent "
                        "Table, or specify the foreign_keys parameter " 
                        "to this relationship."
                        % (
                            primary and 'primaryjoin' or 'secondaryjoin', 
                            join_condition, 
                            self.prop
                        ))

    def _determine_direction(self):
        """Determine if this relationship is one to many, many to one, 
        many to many.

        """
        if self.secondaryjoin is not None:
            self.direction = MANYTOMANY
        else:
            parentcols = util.column_set(self.parent_selectable.c)
            targetcols = util.column_set(self.child_selectable.c)

            # fk collection which suggests ONETOMANY.
            onetomany_fk = targetcols.intersection(
                            self.foreign_key_columns)

            # fk collection which suggests MANYTOONE.

            manytoone_fk = parentcols.intersection(
                            self.foreign_key_columns)

            if onetomany_fk and manytoone_fk:
                # fks on both sides.  test for overlap of local/remote
                # with foreign key
                onetomany_local = self.remote_side.intersection(self.foreign_key_columns)
                manytoone_local = self.local_columns.intersection(self.foreign_key_columns)
                if onetomany_local and not manytoone_local:
                    self.direction = ONETOMANY
                elif manytoone_local and not onetomany_local:
                    self.direction = MANYTOONE
                else:
                    raise sa_exc.ArgumentError(
                            "Can't determine relationship"
                            " direction for relationship '%s' - foreign "
                            "key columns are present in both the parent "
                            "and the child's mapped tables.  Specify "
                            "'foreign_keys' argument." % self.prop)
            elif onetomany_fk:
                self.direction = ONETOMANY
            elif manytoone_fk:
                self.direction = MANYTOONE
            else:
                raise sa_exc.ArgumentError("Can't determine relationship "
                        "direction for relationship '%s' - foreign "
                        "key columns are present in neither the parent "
                        "nor the child's mapped tables" % self.prop)

    @util.memoized_property
    def remote_columns(self):
        return self._gather_join_annotations("remote")

    remote_side = remote_columns

    @util.memoized_property
    def local_columns(self):
        return self._gather_join_annotations("local")

    @util.memoized_property
    def foreign_key_columns(self):
        return self._gather_join_annotations("foreign")

    @util.memoized_property
    def referent_columns(self):
        return self._gather_join_annotations("referent")

    def _gather_join_annotations(self, annotation):
        s = set(
            self._gather_columns_with_annotation(self.primaryjoin, 
                                                    annotation)
        )
        if self.secondaryjoin is not None:
            s.update(
                self._gather_columns_with_annotation(self.secondaryjoin, 
                                                    annotation)
            )
        return s

    def _gather_columns_with_annotation(self, clause, *annotation):
        annotation = set(annotation)
        return set([
            col for col in visitors.iterate(clause, {})
            if annotation.issubset(col._annotations)
        ])

    @util.memoized_property
    def local_remote_pairs(self):
        lrp = []
        def visit_binary(binary):
            if "remote" in binary.right._annotations and \
                "local" in binary.left._annotations:
                lrp.append((binary.left, binary.right))
            elif "remote" in binary.left._annotations and \
                "local" in binary.right._annotations:
                lrp.append((binary.right, binary.left))
        visitors.traverse(self.primaryjoin, {}, {"binary":visit_binary})
        if self.secondaryjoin is not None:
            visitors.traverse(self.secondaryjoin, {}, {"binary":visit_binary})
        return lrp

    def join_targets(self, source_selectable, 
                            dest_selectable,
                            aliased,
                            single_crit=None):
        """Given a source and destination selectable, create a
        join between them.

        This takes into account aliasing the join clause
        to reference the appropriate corresponding columns
        in the target objects, as well as the extra child
        criterion, equivalent column sets, etc.

        """

        # place a barrier on the destination such that
        # replacement traversals won't ever dig into it.
        # its internal structure remains fixed 
        # regardless of context.
        dest_selectable = _shallow_annotate(
                                dest_selectable, 
                                {'no_replacement_traverse':True})

        primaryjoin, secondaryjoin, secondary = self.primaryjoin, \
            self.secondaryjoin, self.secondary

        # adjust the join condition for single table inheritance,
        # in the case that the join is to a subclass
        # this is analogous to the "_adjust_for_single_table_inheritance()"
        # method in Query.

        if single_crit is not None:
            if secondaryjoin is not None:
                secondaryjoin = secondaryjoin & single_crit
            else:
                primaryjoin = primaryjoin & single_crit

        if aliased:
            if secondary is not None:
                secondary = secondary.alias()
                primary_aliasizer = ClauseAdapter(secondary)
                secondary_aliasizer = \
                    ClauseAdapter(dest_selectable,
                        equivalents=self.child_equivalents).\
                        chain(primary_aliasizer)
                if source_selectable is not None:
                    primary_aliasizer = \
                        ClauseAdapter(secondary).\
                            chain(ClauseAdapter(source_selectable,
                            equivalents=self.parent_equivalents))
                secondaryjoin = \
                    secondary_aliasizer.traverse(secondaryjoin)
            else:
                primary_aliasizer = ClauseAdapter(dest_selectable,
                        exclude_fn=lambda c: "local" in c._annotations,
                        equivalents=self.child_equivalents)
                if source_selectable is not None:
                    primary_aliasizer.chain(
                        ClauseAdapter(source_selectable,
                            exclude_fn=lambda c: "remote" in c._annotations,
                            equivalents=self.parent_equivalents))
                secondary_aliasizer = None

            primaryjoin = primary_aliasizer.traverse(primaryjoin)
            target_adapter = secondary_aliasizer or primary_aliasizer
            target_adapter.exclude_fn = None
        else:
            target_adapter = None
        return primaryjoin, secondaryjoin, secondary, target_adapter

################# everything below is TODO ################################

def _create_lazy_clause(cls, prop, reverse_direction=False):
    binds = util.column_dict()
    lookup = util.column_dict()
    equated_columns = util.column_dict()

    if reverse_direction and prop.secondaryjoin is None:
        for l, r in prop.local_remote_pairs:
            _list = lookup.setdefault(r, [])
            _list.append((r, l))
            equated_columns[l] = r
    else:
        for l, r in prop.local_remote_pairs:
            _list = lookup.setdefault(l, [])
            _list.append((l, r))
            equated_columns[r] = l

    def col_to_bind(col):
        if col in lookup:
            for tobind, equated in lookup[col]:
                if equated in binds:
                    return None
            if col not in binds:
                binds[col] = sql.bindparam(None, None, type_=col.type, unique=True)
            return binds[col]
        return None

    lazywhere = prop.primaryjoin

    if prop.secondaryjoin is None or not reverse_direction:
        lazywhere = visitors.replacement_traverse(
                                        lazywhere, {}, col_to_bind) 

    if prop.secondaryjoin is not None:
        secondaryjoin = prop.secondaryjoin
        if reverse_direction:
            secondaryjoin = visitors.replacement_traverse(
                                        secondaryjoin, {}, col_to_bind)
        lazywhere = sql.and_(lazywhere, secondaryjoin)

    bind_to_col = dict((binds[col].key, col) for col in binds)

    return lazywhere, bind_to_col, equated_columns

def _sync_pairs_from_join(self, join_condition, primary):
    """Determine a list of "source"/"destination" column pairs
    based on the given join condition, as well as the
    foreign keys argument.

    "source" would be a column referenced by a foreign key,
    and "destination" would be the column who has a foreign key
    reference to "source".

    """

    fks = self._user_defined_foreign_keys
    # locate pairs
    eq_pairs = criterion_as_pairs(join_condition,
            consider_as_foreign_keys=fks,
            any_operator=self.viewonly)

    # couldn't find any fks, but we have 
    # "secondary" - assume the "secondary" columns
    # are the fks
    if not eq_pairs and \
            self.secondary is not None and \
            not fks:
        fks = set(self.secondary.c)
        eq_pairs = criterion_as_pairs(join_condition,
                consider_as_foreign_keys=fks,
                any_operator=self.viewonly)

        if eq_pairs:
            util.warn("No ForeignKey objects were present "
                        "in secondary table '%s'.  Assumed referenced "
                        "foreign key columns %s for join condition '%s' "
                        "on relationship %s" % (
                        self.secondary.description,
                        ", ".join(sorted(["'%s'" % col for col in fks])),
                        join_condition,
                        self
                    ))

    # Filter out just to columns that are mapped.
    # If viewonly, allow pairs where the FK col
    # was part of "foreign keys" - the column it references
    # may be in an un-mapped table - see 
    # test.orm.test_relationships.ViewOnlyComplexJoin.test_basic
    # for an example of this.
    eq_pairs = [(l, r) for (l, r) in eq_pairs
                if self._columns_are_mapped(l, r)
                or self.viewonly and 
                r in fks]

    if eq_pairs:
        return eq_pairs

    # from here below is just determining the best error message
    # to report.  Check for a join condition using any operator 
    # (not just ==), perhaps they need to turn on "viewonly=True".
    if not self.viewonly and criterion_as_pairs(join_condition,
            consider_as_foreign_keys=self._user_defined_foreign_keys,
            any_operator=True):

        err = "Could not locate any "\
                "foreign-key-equated, locally mapped column "\
                "pairs for %s "\
                "condition '%s' on relationship %s." % (
                    primary and 'primaryjoin' or 'secondaryjoin', 
                    join_condition, 
                    self
                )

        if not self._user_defined_foreign_keys:
            err += "  Ensure that the "\
                    "referencing Column objects have a "\
                    "ForeignKey present, or are otherwise part "\
                    "of a ForeignKeyConstraint on their parent "\
                    "Table, or specify the foreign_keys parameter "\
                    "to this relationship."

        err += "  For more "\
                "relaxed rules on join conditions, the "\
                "relationship may be marked as viewonly=True."

        raise sa_exc.ArgumentError(err)
    else:
        if self._user_defined_foreign_keys:
            raise sa_exc.ArgumentError("Could not determine "
                    "relationship direction for %s condition "
                    "'%s', on relationship %s, using manual "
                    "'foreign_keys' setting.  Do the columns "
                    "in 'foreign_keys' represent all, and "
                    "only, the 'foreign' columns in this join "
                    "condition?  Does the %s Table already "
                    "have adequate ForeignKey and/or "
                    "ForeignKeyConstraint objects established "
                    "(in which case 'foreign_keys' is usually "
                    "unnecessary)?" 
                    % (
                        primary and 'primaryjoin' or 'secondaryjoin',
                        join_condition, 
                        self,
                        primary and 'mapped' or 'secondary'
                    ))
        else:
            raise sa_exc.ArgumentError("Could not determine "
                    "relationship direction for %s condition "
                    "'%s', on relationship %s. Ensure that the "
                    "referencing Column objects have a "
                    "ForeignKey present, or are otherwise part "
                    "of a ForeignKeyConstraint on their parent "
                    "Table, or specify the foreign_keys parameter " 
                    "to this relationship."
                    % (
                        primary and 'primaryjoin' or 'secondaryjoin', 
                        join_condition, 
                        self
                    ))

def _determine_synchronize_pairs(self):
    """Resolve 'primary'/foreign' column pairs from the primaryjoin
    and secondaryjoin arguments.

    """
    if self.local_remote_pairs:
        if not self._user_defined_foreign_keys:
            raise sa_exc.ArgumentError(
                    "foreign_keys argument is "
                    "required with _local_remote_pairs argument")
        self.synchronize_pairs = []
        for l, r in self.local_remote_pairs:
            if r in self._user_defined_foreign_keys:
                self.synchronize_pairs.append((l, r))
            elif l in self._user_defined_foreign_keys:
                self.synchronize_pairs.append((r, l))
    else:
        self.synchronize_pairs = self._sync_pairs_from_join(
                                            self.primaryjoin, 
                                            True)

    self._calculated_foreign_keys = util.column_set(
                            r for (l, r) in
                            self.synchronize_pairs)

    if self.secondaryjoin is not None:
        self.secondary_synchronize_pairs = self._sync_pairs_from_join(
                                                    self.secondaryjoin, 
                                                    False)
        self._calculated_foreign_keys.update(
                            r for (l, r) in
                            self.secondary_synchronize_pairs)
    else:
        self.secondary_synchronize_pairs = None

def _determine_direction(self):
    """Determine if this relationship is one to many, many to one, 
    many to many.

    This is derived from the primaryjoin, presence of "secondary",
    and in the case of self-referential the "remote side".

    """
    if self.secondaryjoin is not None:
        self.direction = MANYTOMANY
    elif self._refers_to_parent_table():

        # self referential defaults to ONETOMANY unless the "remote"
        # side is present and does not reference any foreign key
        # columns

        if self.local_remote_pairs:
            remote = [r for (l, r) in self.local_remote_pairs]
        elif self.remote_side:
            remote = self.remote_side
        else:
            remote = None
        if not remote or self._calculated_foreign_keys.difference(l for (l,
                r) in self.synchronize_pairs).intersection(remote):
            self.direction = ONETOMANY
        else:
            self.direction = MANYTOONE
    else:
        parentcols = util.column_set(self.parent.mapped_table.c)
        targetcols = util.column_set(self.mapper.mapped_table.c)

        # fk collection which suggests ONETOMANY.
        onetomany_fk = targetcols.intersection(
                        self._calculated_foreign_keys)

        # fk collection which suggests MANYTOONE.

        manytoone_fk = parentcols.intersection(
                        self._calculated_foreign_keys)

        if onetomany_fk and manytoone_fk:
            # fks on both sides.  do the same test only based on the
            # local side.
            referents = [c for (c, f) in self.synchronize_pairs]
            onetomany_local = parentcols.intersection(referents)
            manytoone_local = targetcols.intersection(referents)

            if onetomany_local and not manytoone_local:
                self.direction = ONETOMANY
            elif manytoone_local and not onetomany_local:
                self.direction = MANYTOONE
            else:
                raise sa_exc.ArgumentError(
                        "Can't determine relationship"
                        " direction for relationship '%s' - foreign "
                        "key columns are present in both the parent "
                        "and the child's mapped tables.  Specify "
                        "'foreign_keys' argument." % self)
        elif onetomany_fk:
            self.direction = ONETOMANY
        elif manytoone_fk:
            self.direction = MANYTOONE
        else:
            raise sa_exc.ArgumentError("Can't determine relationship "
                    "direction for relationship '%s' - foreign "
                    "key columns are present in neither the parent "
                    "nor the child's mapped tables" % self)

    if self.cascade.delete_orphan and not self.single_parent \
        and (self.direction is MANYTOMANY or self.direction
             is MANYTOONE):
        util.warn('On %s, delete-orphan cascade is not supported '
                  'on a many-to-many or many-to-one relationship '
                  'when single_parent is not set.   Set '
                  'single_parent=True on the relationship().'
                  % self)
    if self.direction is MANYTOONE and self.passive_deletes:
        util.warn("On %s, 'passive_deletes' is normally configured "
                  "on one-to-many, one-to-one, many-to-many "
                  "relationships only."
                   % self)

def _determine_local_remote_pairs(self):
    """Determine pairs of columns representing "local" to 
    "remote", where "local" columns are on the parent mapper,
    "remote" are on the target mapper.

    These pairs are used on the load side only to generate
    lazy loading clauses.

    """
    if not self.local_remote_pairs and not self.remote_side:
        # the most common, trivial case.   Derive 
        # local/remote pairs from the synchronize pairs.
        eq_pairs = util.unique_list(
                        self.synchronize_pairs + 
                        (self.secondary_synchronize_pairs or []))
        if self.direction is MANYTOONE:
            self.local_remote_pairs = [(r, l) for l, r in eq_pairs]
        else:
            self.local_remote_pairs = eq_pairs

    # "remote_side" specified, derive from the primaryjoin
    # plus remote_side, similarly to how synchronize_pairs
    # were determined.
    elif self.remote_side:
        if self.local_remote_pairs:
            raise sa_exc.ArgumentError('remote_side argument is '
                'redundant against more detailed '
                '_local_remote_side argument.')
        if self.direction is MANYTOONE:
            self.local_remote_pairs = [(r, l) for (l, r) in
                    criterion_as_pairs(self.primaryjoin,
                    consider_as_referenced_keys=self.remote_side,
                    any_operator=True)]

        else:
            self.local_remote_pairs = \
                criterion_as_pairs(self.primaryjoin,
                    consider_as_foreign_keys=self.remote_side,
                    any_operator=True)
        if not self.local_remote_pairs:
            raise sa_exc.ArgumentError('Relationship %s could '
                    'not determine any local/remote column '
                    'pairs from remote side argument %r'
                    % (self, self.remote_side))
    # else local_remote_pairs were sent explcitly via
    # ._local_remote_pairs.

    # create local_side/remote_side accessors
    self.local_side = util.ordered_column_set(
                        l for l, r in self.local_remote_pairs)
    self.remote_side = util.ordered_column_set(
                        r for l, r in self.local_remote_pairs)

    # check that the non-foreign key column in the local/remote
    # collection is mapped.  The foreign key
    # which the individual mapped column references directly may
    # itself be in a non-mapped table; see
    # test.orm.test_relationships.ViewOnlyComplexJoin.test_basic
    # for an example of this.
    if self.direction is ONETOMANY:
        for col in self.local_side:
            if not self._columns_are_mapped(col):
                raise sa_exc.ArgumentError(
                        "Local column '%s' is not "
                        "part of mapping %s.  Specify remote_side "
                        "argument to indicate which column lazy join "
                        "condition should compare against." % (col,
                        self.parent))
    elif self.direction is MANYTOONE:
        for col in self.remote_side:
            if not self._columns_are_mapped(col):
                raise sa_exc.ArgumentError(
                        "Remote column '%s' is not "
                        "part of mapping %s. Specify remote_side "
                        "argument to indicate which column lazy join "
                        "condition should bind." % (col, self.mapper))

    count = [0]
    def clone(elem):
        if set(['local', 'remote']).intersection(elem._annotations):
            return None
        elif elem in self.local_side and elem in self.remote_side:
            # TODO: OK this still sucks.  this is basically,
            # refuse, refuse, refuse the temptation to guess!
            # but crap we really have to guess don't we.  we 
            # might want to traverse here with cloned_traverse
            # so we can see the binary exprs and do it at that 
            # level....
            if count[0] % 2 == 0:
                elem = elem._annotate({'local':True})
            else:
                elem = elem._annotate({'remote':True})
            count[0] += 1
        elif elem in self.local_side:
            elem = elem._annotate({'local':True})
        elif elem in self.remote_side:
            elem = elem._annotate({'remote':True})
        else:
            elem = None
        return elem

    self.primaryjoin = visitors.replacement_traverse(
                            self.primaryjoin, {}, clone
                        )


def _criterion_exists(self, criterion=None, **kwargs):
    if getattr(self, '_of_type', None):
        target_mapper = self._of_type
        to_selectable = target_mapper._with_polymorphic_selectable
        if self.property._is_self_referential:
            to_selectable = to_selectable.alias()

        single_crit = target_mapper._single_table_criterion
        if single_crit is not None:
            if criterion is not None:
                criterion = single_crit & criterion
            else:
                criterion = single_crit
    else:
        to_selectable = None

    if self.adapter:
        source_selectable = self.__clause_element__()
    else:
        source_selectable = None

    pj, sj, source, dest, secondary, target_adapter = \
        self.property._create_joins(dest_polymorphic=True,
                dest_selectable=to_selectable,
                source_selectable=source_selectable)

    for k in kwargs:
        crit = getattr(self.property.mapper.class_, k) == kwargs[k]
        if criterion is None:
            criterion = crit
        else:
            criterion = criterion & crit

    # annotate the *local* side of the join condition, in the case
    # of pj + sj this is the full primaryjoin, in the case of just
    # pj its the local side of the primaryjoin.
    if sj is not None:
        j = _orm_annotate(pj) & sj
    else:
        j = _orm_annotate(pj, exclude=self.property.remote_side)

    # MARKMARK
    if criterion is not None and target_adapter:
        # limit this adapter to annotated only?
        criterion = target_adapter.traverse(criterion)

    # only have the "joined left side" of what we 
    # return be subject to Query adaption.  The right
    # side of it is used for an exists() subquery and 
    # should not correlate or otherwise reach out
    # to anything in the enclosing query.
    if criterion is not None:
        criterion = criterion._annotate({'no_replacement_traverse': True})

    crit = j & criterion

    return sql.exists([1], crit, from_obj=dest).\
                    correlate(source._annotate({'_orm_adapt':True}))


def __negated_contains_or_equals(self, other):
    if self.property.direction == MANYTOONE:
        state = attributes.instance_state(other)

        def state_bindparam(x, state, col):
            o = state.obj() # strong ref
            return sql.bindparam(x, unique=True, callable_=lambda : \
                self.property.mapper._get_committed_attr_by_column(o,
                    col))

        def adapt(col):
            if self.adapter:
                return self.adapter(col)
            else:
                return col

        if self.property._use_get:
            return sql.and_(*[
                sql.or_(
                adapt(x) != state_bindparam(adapt(x), state, y),
                adapt(x) == None)
                for (x, y) in self.property.local_remote_pairs])

    criterion = sql.and_(*[x==y for (x, y) in 
                        zip(
                            self.property.mapper.primary_key,
                            self.property.\
                                    mapper.\
                                    primary_key_from_instance(other))
                            ])
    return ~self._criterion_exists(criterion)
