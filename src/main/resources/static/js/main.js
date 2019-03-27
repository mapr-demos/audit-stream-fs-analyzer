var page;

window.onload = function () {
    $('#page-size').val('20');
    getContent(0);

    $('#pagination').on("page", function(event, num) {
        getContent(num - 1);
    });

    function updateContent(content) {
        $("#content").empty();
        $.each(content, function(index, value){
            $("#content-table").append('<tr><td>' + value.fid + '</td><td>' + value.name + '</td><td>' +  value.path +
            '</td><td>' + formatDate(value.accessDate) + '</td><td>' + formatDate(value.updateDate) + '</td></tr>')
        });
    }

    function getContent(pageNumber) {
        var uri = getRequestParametersUri(pageNumber);
        $.get('/fileinfo/paginated' + uri, function(data) {
              updateContent(data.content);
              $('#pagination').bootpag({
                      total: data.totalPages != 0 ? data.totalPages : 1,
                      page: data.number + 1,
                      maxVisible: 1,
                      leaps: true,
                      next: "Next",
                      prev: "Previous",
                      wrapClass: 'pagination',
                      activeClass: 'active',
                      disabledClass: 'disabled',
                      nextClass: 'next',
                      prevClass: 'prev',
                      lastClass: 'last',
                      firstClass: 'first',
                      //href: getRequestParametersUri("{{number}}"),
              });
        });
    }

    function formatDate(date) {
        if(date == null)
            return "";
        else
            return moment(date).format("DD-MM-YYYY HH:mm:ss");
    }

    function getRequestParametersUri(pageNumber) {
        var uri = '?page=' + pageNumber;

        var size = $("#page-size").val()
        if(size != '')
           uri += '&size=' + size;

        var fid = $("#searchFid").val();
        if(fid != '')
            uri += '&fid=' + fid;

        var name = $("#searchName").val();
        if(name != '')
            uri += '&name=' + name;

        var path = $("#searchPath").val();
        if(path != '')
            uri += '&path=' + path;

        var accessDate = $("#searchAccessDate").val();
        if(accessDate != '')
            uri += '&accessDate=' + moment(accessDate, 'DD/MM/YY HH:mm:ss').utc().unix() * 1000;

        var updateDate = $("#searchUpdateDate").val();
        if(updateDate != '')
            uri += '&updateDate=' + moment(updateDate, 'DD/MM/YY HH:mm:ss').utc().unix() * 1000;

        var previous;
        if(pageNumber != 0 && pageNumber < page) {
            previous = true;
            uri += '&previous=true';
        }

        var lastFid;
        if(previous)
            lastFid = $('#content').find('tr:first td:first').text();
        else
            lastFid = $('#content-table').find('tr:last td:first').text();
        if(pageNumber != 0 && lastFid != '')
           uri += '&lastFid=' + lastFid;

        page = pageNumber;

        return uri;
    }

    $(function () {
        $('#datetimepickerAccessDate').datetimepicker({
            format: 'DD/MM/YY HH:mm:ss',
        });
    });

    $(function () {
        $('#datetimepickerUpdateDate').datetimepicker({
            format: 'DD/MM/YY HH:mm:ss',
        });
    });

    $(function () {
        $("form").on('submit', function (e) {
            e.preventDefault();
        });
        $("form").on('reset', function (e) {
                    e.preventDefault();
                });
        $( "#search" ).click(function() { getContent(0); });
        $( "#clear" ).click(function() {
            $("#searchFid").val('');
            $("#searchName").val('');
            $("#searchPath").val('');
            $("#searchAccessDate").val('');
            $("#searchUpdateDate").val('');
        });
    });

}

