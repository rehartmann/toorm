package require tdbc
package require logger
package provide toorm 0.1

namespace eval toorm {

    namespace export idVariable normalizeClass classNameToDatabase variableNameToDatabase EntityManager

    proc idVariable {class v} {
        set ::toorm::idVars([uplevel 1 normalizeClass $class]) $v
    }

    proc normalizeClass {class} {
        if {[string match {::*} $class]} {
            return $class
        }
        if {[uplevel 1 namespace current] == {::}} {
            return ::$class
        }
        return [uplevel 1 namespace current]::$class
    }

    proc classNameToDatabase {name} {
        set name [namespace tail $name]
        set result {}
        for {set i 0} {$i < [string length $name]} {incr i} {
            set c [string index $name $i]
            if {[string is upper $c] && $i > 0} {
                append result _[string tolower $c]
            } else {
                append result [string tolower $c]
            }
        }
        return ${result}s
    }

    proc variableNameToDatabase {name} {
        set name [namespace tail $name]
        set result {}
        for {set i 0} {$i < [string length $name]} {incr i} {
            set c [string index $name $i]
            if {[string is upper $c] && $i > 0} {
                append result _[string tolower $c]
            } else {
                append result $c
            }
        }
        return $result
    }

    proc recordUpdate {emns obj v args} {
        lappend ${emns}::dirtyVars $obj $v
    }

    proc recordDelete {emns class id args} {
        lappend ${emns}::deleted $class $id
        array unset ${emns}::entities $class,$id
    }

    variable log {}
    
    oo::class create EntityManager {
        variable connection

        # An array which holds all managed entities.
        # The key is class,id
        variable entities
        
        # A list of persisted entities which are inserted into the database
        # at transaction commit.
        variable persisted

        # A list containing objects and variable names which are written to the database on commit
        variable dirtyVars
    
        # A list containing classes and ids of deleted objects
        variable deleted
    
        variable txActive
               
        constructor {conn} {
            set connection $conn
            set persisted {}
            set dirtyVars {}
            set deleted {}
            set txActive 0
            if {$::toorm::log == {}} {
                set ::toorm::log [logger::init toorm]
            }
        }

        destructor {
            if {$txActive} {
                my rollback
            }

            # Destroy all managed entities
            foreach key [array names entities] {
                $entities($key) destroy
            }
            
            # Destroy queries
            foreach query [info class instances ::toorm::NativeQuery] {
                if {[$query getEntityManager] == [self object]} {
                    $query destroy
                }
            }
        }

        method getConnection {} {
            return $connection
        }

        method find {class id} {
            set class [uplevel 1 ::toorm::normalizeClass $class]
            if {[info exists entities($class,$id)]} {
                return $entities($class,$id)
            }
            set sql "SELECT * FROM [::toorm::classNameToDatabase $class]
                    WHERE [::toorm::variableNameToDatabase $::toorm::idVars($class)] = :id"
            ${::toorm::log}::debug "SQL: $sql"
            set stmt [$connection prepare $sql]
            try {
                set res [$stmt execute]
                try {
                    if {[$res nextdict resdict] == 0} {
                        return {}
                    }
                } finally {
                    $res close
                }
            } finally {
                $stmt close
            }
            return [my AddEntity $class $resdict] 
        }

        method createNativeQuery {query class} {
            set stmt [$connection prepare $query]
            return [::toorm::NativeQuery new [self object] $stmt [uplevel 1 ::toorm::normalizeClass $class]]
        }

        method detach {obj} {
            set class [info object class $obj]
            unset entities($class,[my ObjId $obj])
            foreach v [info class variables $class] {
                set varname [info object namespace $obj]::$v
                trace remove variable $varname write [list toorm::recordUpdate [self namespace] $obj $v]
            }
            trace remove command $obj delete [list toorm::recordDelete [self namespace] $class [my ObjId $obj]]
        }

        method persist {obj} {
            set class [info object class $obj]
            if {[info exists entities($class,[my ObjId $obj])]} {
                error "object already exists"
            }
            lappend persisted $obj
        }
        
        method beginTransaction {} {
            if {$txActive} {
                error "nested transactions are not suppported"
            }
            $connection begintransaction
            set persisted {}
            set dirtyVars {}
            set deleted {}
            set txActive 1
        }
        
        method commit {} {
            if {!$txActive} {
                error "transaction not active"
            }
            my flush
            $connection commit
            set txActive 0
        }
        
        method rollback {} {
            if {!$txActive} {
                error "transaction not active"
            }
            set persisted {}
            set dirtyVars {}
            set deleted {}
            $connection rollback
            set txActive 0
        }

        method flush {} {
            foreach obj $persisted {
                # Check if the object still exists
                if {[info object isa object $obj]} {
                    # Insert row
                    set class [info object class $obj]
                    set vars [info class variables $class]
                    set stmt [$connection prepare [my InsertStmt $class $vars]]
                    try {
                        set values [dict create]
                        foreach v $vars {
                            dict set values [::toorm::variableNameToDatabase $v] [set [info object namespace $obj]::$v]
                        }
                        $stmt execute $values
                    } finally {
                        $stmt close
                    }
                }
            }
            set persisted {}
            foreach {obj var} $dirtyVars {
                if {[info object isa object $obj]} {
                    set stmt [$connection prepare [my UpdateStmt [info object class $obj] $var]]
                    try {
                        $stmt execute [list [::toorm::variableNameToDatabase $var] \
                                [set [info object namespace $obj]::$var] \
                                id_ [my ObjId $obj]]
                    } finally {
                        $stmt close
                    }
                }
            }
            set dirtyVars {}
            foreach {class id} $deleted {
                set stmt [$connection prepare [my DeleteStmt $class]]
                try {
                    $stmt execute [list id_ $id]
                } finally {
                    $stmt close
                }
            }
            set deleted {}
        }

        method AddEntity {class data} {
            set id [dict get $data [::toorm::variableNameToDatabase $::toorm::idVars($class)]]
            # Reuse object if it is already managed
            if {[info exists entities($class,$id)]} {
                set resobj $entities($class,$id)
                foreach v [info class variables $class] {
                    set varname [info object namespace $resobj]::$v
                    trace remove variable $varname write [list toorm::recordUpdate [self namespace] $resobj $v]
                }
                trace remove command $resobj delete [list toorm::recordDelete [self namespace] $class [my ObjId $resobj]]
            } else {
                set resobj [$class new]
            }
            foreach v [info class variables $class] {
                set varname [info object namespace $resobj]::$v
                set $varname [dict get $data [::toorm::variableNameToDatabase $v]]
                trace add variable $varname write [list toorm::recordUpdate [self namespace] $resobj $v]
            }
            trace add command $resobj delete [list toorm::recordDelete [self namespace] $class [my ObjId $resobj]]
            set entities($class,[my ObjId $resobj]) $resobj
            return $resobj
        }

        method InsertStmt {class vars} {
            set s "INSERT INTO [classNameToDatabase $class] ("
            set first 1
            foreach v $vars {
                if {$first} {
                set first 0
                } else {
                append s {, }
                }
                append s [variableNameToDatabase $v]
            }
            append s ") VALUES ("
            set first 1
            foreach v $vars {
                if {$first} {
                set first 0
                } else {
                append s {, }
                }
                append s :
                append s [variableNameToDatabase $v]
            }
            append s )
            ${::toorm::log}::debug "SQL: $s"
            return $s
        }

        method UpdateStmt {class v} {
            set s "UPDATE "
            append s [classNameToDatabase $class]
            append s " SET "
            append s [variableNameToDatabase $v]
            append s " = :"
            append s [variableNameToDatabase $v]
            append s " WHERE "
            append s $::toorm::idVars($class)
            append s " = :id_"
            ${::toorm::log}::debug "SQL: $s"
            return $s
        }

        method DeleteStmt {class} {
            set s "DELETE FROM "
            append s [classNameToDatabase $class]
            append s " WHERE "
            append s $::toorm::idVars($class)
            append s " = :id_"
            ${::toorm::log}::debug "SQL: $s"
            return $s
        }

        method ObjId {obj} {
            set class [info object class $obj]
            return [set [info object namespace $obj]::$::toorm::idVars($class)]
        }
    }

    oo::class create NativeQuery {
        variable entityManager
        variable stmt
        variable class
        
        constructor {em s cl} {
            set entityManager $em
            set stmt $s
            set class $cl
        }

        destructor {
            $stmt destroy
        }
        
        method getEntityManager {} {
            return $entityManager
        }
        
        method getResults {} {
            set res {}
            $stmt foreach row {
                lappend res [[info object namespace $entityManager]::my AddEntity $class $row]
            }
            return $res
        }
    }

}
