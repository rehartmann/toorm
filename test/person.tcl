oo::class create Person {
    variable id
    variable name

    method print {} {
        puts "id: $id, name: $name"
    }

    method setId {i} {
        set id $i
    }

    method getId {} {
        return $id
    }

    method setName {n} {
        set name $n
    }
    
    method getName {} {
        return $name
    }
}
